"""Pure-function tests for the deploy helpers: no Terraform, Confluent, or AWS calls."""

import json
import re
from pathlib import Path

import pytest

from scripts import setup_rtce
from scripts.mcp_setup import _TF_TO_MCP, _build_env_vars
from scripts.tfvars import (
    DEMO_PIPELINE_MODULES,
    deployed_demo_pipeline,
    deployed_llm_provider,
    generate_tfvars_content,
    is_workshop_mode,
    llm_provider_for,
    saved_llm_provider,
    write_tfvars_for_deployment,
)

ROOT = Path(__file__).resolve().parent.parent
CREDS = {
    "TF_VAR_confluent_cloud_api_key": "CCKEY",
    "TF_VAR_confluent_cloud_api_secret": "CCSECRET",
    "TF_VAR_aws_bedrock_access_key": "AKIAEXAMPLE",
    "TF_VAR_aws_bedrock_secret_key": "AWSSECRET",
}


def _declared(kind: str, *files: str) -> set[str]:
    text = "".join((ROOT / "terraform" / f).read_text() for f in files)
    return set(re.findall(rf'^{kind} "([^"]+)"', text, re.M))


def test_tfvars_sets_only_declared_variables():
    content = generate_tfvars_content(
        "k", "s", "a", "b", aws_session_token="t", deploy_flink_pipeline=False, llm_provider="bedrock"
    )
    names = set(re.findall(r"^(\w+)\s*=", content, re.M))
    assert names <= _declared("variable", "variables.tf")
    assert "deploy_flink_pipeline      = false" in content
    assert 'llm_provider               = "bedrock"' in content


def test_tfvars_csp_default_omits_aws_keys():
    content = generate_tfvars_content("k", "s", "a", "b", aws_session_token="t")
    assert 'llm_provider               = "confluent"' in content
    assert "aws_" not in content


def test_workshop_mode_flag():
    assert not is_workshop_mode(CREDS)
    assert not is_workshop_mode({**CREDS, "TF_VAR_deploy_flink_pipeline": "true"})
    assert is_workshop_mode({**CREDS, "TF_VAR_deploy_flink_pipeline": "false"})


def test_write_tfvars_requires_bedrock_keys_only_for_bedrock(tmp_path):
    (tmp_path / "terraform").mkdir()
    bedrock = {**CREDS, "TF_VAR_llm_provider": "bedrock"}
    assert not write_tfvars_for_deployment(tmp_path, {**bedrock, "TF_VAR_aws_bedrock_secret_key": ""})
    assert write_tfvars_for_deployment(tmp_path, bedrock)
    assert 'aws_bedrock_access_key     = "AKIAEXAMPLE"' in (tmp_path / "terraform" / "terraform.tfvars").read_text()

    csp = {k: v for k, v in CREDS.items() if not k.startswith("TF_VAR_aws_")}
    assert write_tfvars_for_deployment(tmp_path, csp)
    assert 'llm_provider               = "confluent"' in (tmp_path / "terraform" / "terraform.tfvars").read_text()
    assert not write_tfvars_for_deployment(tmp_path, {**csp, "TF_VAR_confluent_cloud_api_secret": ""})


def test_saved_llm_provider_defaults_to_csp_and_rejects_unknown():
    assert saved_llm_provider({}) == "confluent"
    assert saved_llm_provider({"TF_VAR_llm_provider": "bedrock"}) == "bedrock"
    with pytest.raises(ValueError):
        saved_llm_provider({"TF_VAR_llm_provider": "openai"})


def _write_state(root: Path, *names: str) -> None:
    (root / "terraform").mkdir(exist_ok=True)
    resources = [{"name": n, "instances": [{}]} for n in names]
    (root / "terraform" / "terraform.tfstate").write_text(json.dumps({"resources": resources}))


def test_deployed_llm_provider_reads_state(tmp_path):
    assert deployed_llm_provider(tmp_path) is None
    _write_state(tmp_path)
    assert deployed_llm_provider(tmp_path) is None
    _write_state(tmp_path, "main", "this", "bedrock")
    assert deployed_llm_provider(tmp_path) == "bedrock"
    _write_state(tmp_path, "main", "this")
    assert deployed_llm_provider(tmp_path) == "confluent"


def test_deployed_demo_pipeline_reads_state(tmp_path):
    assert not deployed_demo_pipeline(tmp_path)
    tf = tmp_path / "terraform"

    def write(*modules: str) -> None:
        resources = [{"module": m, "name": "this", "instances": [{}]} for m in modules]
        (tf / "terraform.tfstate").write_text(json.dumps({"resources": resources}))

    tf.mkdir()
    write("module.tbl_fraud_alerts", "module.fn_flag")  # workshop mode
    assert not deployed_demo_pipeline(tmp_path)
    write("module.tbl_fraud_alerts", "module.model[0]")  # demo mode
    assert deployed_demo_pipeline(tmp_path)


def test_demo_pipeline_modules_match_terraform():
    text = (ROOT / "terraform" / "flink.tf").read_text()
    gated = re.findall(r'^module "(\w+)" \{\n[^\n]*\n\s*count\s*=\s*var\.deploy_flink_pipeline', text, re.M)
    assert set(gated) == set(DEMO_PIPELINE_MODULES)


def test_llm_provider_for_prefers_request_then_state_then_saved(tmp_path):
    assert llm_provider_for(tmp_path, {}) == "confluent"
    assert llm_provider_for(tmp_path, {"TF_VAR_llm_provider": "bedrock"}) == "bedrock"
    # A pre-existing Bedrock deployment (no provider saved) stays on Bedrock.
    _write_state(tmp_path, "main", "bedrock")
    assert llm_provider_for(tmp_path, {}) == "bedrock"
    # An explicit request still wins over the state.
    assert llm_provider_for(tmp_path, {}, "confluent") == "confluent"
    with pytest.raises(ValueError):
        llm_provider_for(tmp_path, {}, "openai")


def test_bedrock_connection_is_gated_on_provider():
    flink = (ROOT / "terraform" / "flink.tf").read_text()
    assert "count = local.use_bedrock ? 1 : 0" in flink
    assert "to   = confluent_flink_connection.bedrock[0]" in flink
    assert "'confluent.model' = '${var.csp_hosted_model}'" in flink


def test_terraform_exposes_every_output_setup_mcp_reads():
    assert set(_TF_TO_MCP) <= _declared("output", "outputs.tf")


def test_mcp_env_strips_bootstrap_scheme():
    env = _build_env_vars({"confluent_kafka_cluster_bootstrap_endpoint": "SASL_SSL://host:9092"})
    assert env["BOOTSTRAP_SERVERS"] == "host:9092"


def test_terraform_exposes_every_output_setup_rtce_reads():
    names = {getattr(setup_rtce, n) for n in dir(setup_rtce) if n.startswith("_TF_")}
    assert names | {"source_topics", "served_topics"} <= _declared("output", "outputs.tf")


@pytest.mark.parametrize(
    ("interactive", "answer", "destroy_rc", "exits", "destroyed"),
    [
        (True, "y", 0, False, True),  # destroyed, deploy continues
        (True, "y", 1, True, True),  # destroy failed
        (True, "n", 0, True, False),  # declined
        (False, "y", 0, True, False),  # bots never destroy
    ],
)
def test_start_clean_for_workshop(monkeypatch, tmp_path, interactive, answer, destroy_rc, exits, destroyed):
    from scripts import deploy

    calls = []
    monkeypatch.setattr("builtins.input", lambda _prompt: answer)
    monkeypatch.setattr(
        deploy.subprocess, "run", lambda cmd, cwd: calls.append(cmd) or type("R", (), {"returncode": destroy_rc})()
    )
    if exits:
        with pytest.raises(SystemExit):
            deploy._start_clean_for_workshop(tmp_path, interactive)
    else:
        deploy._start_clean_for_workshop(tmp_path, interactive)
    assert calls == ([["uv", "run", "destroy", "--testing"]] if destroyed else [])
