"""Pure-function tests for the deploy helpers: no Terraform, Confluent, or AWS calls."""

import re
from pathlib import Path

from scripts import setup_rtce
from scripts.mcp_setup import _TF_TO_MCP, _build_env_vars
from scripts.tfvars import generate_tfvars_content, is_workshop_mode, write_tfvars_for_deployment

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
    content = generate_tfvars_content("k", "s", "a", "b", aws_session_token="t", deploy_flink_pipeline=False)
    names = set(re.findall(r"^(\w+)\s*=", content, re.M))
    assert names <= _declared("variable", "variables.tf")
    assert "deploy_flink_pipeline      = false" in content


def test_workshop_mode_flag():
    assert not is_workshop_mode(CREDS)
    assert not is_workshop_mode({**CREDS, "TF_VAR_deploy_flink_pipeline": "true"})
    assert is_workshop_mode({**CREDS, "TF_VAR_deploy_flink_pipeline": "false"})


def test_write_tfvars_requires_all_credentials(tmp_path):
    (tmp_path / "terraform").mkdir()
    assert not write_tfvars_for_deployment(tmp_path, {**CREDS, "TF_VAR_aws_bedrock_secret_key": ""})
    assert write_tfvars_for_deployment(tmp_path, CREDS)
    assert 'aws_bedrock_access_key     = "AKIAEXAMPLE"' in (tmp_path / "terraform" / "terraform.tfvars").read_text()


def test_terraform_exposes_every_output_setup_mcp_reads():
    assert set(_TF_TO_MCP) <= _declared("output", "outputs.tf")


def test_mcp_env_strips_bootstrap_scheme():
    env = _build_env_vars({"confluent_kafka_cluster_bootstrap_endpoint": "SASL_SSL://host:9092"})
    assert env["BOOTSTRAP_SERVERS"] == "host:9092"


def test_terraform_exposes_every_output_setup_rtce_reads():
    names = {getattr(setup_rtce, n) for n in dir(setup_rtce) if n.startswith("_TF_")}
    assert names | {"source_topics", "served_topics"} <= _declared("output", "outputs.tf")
