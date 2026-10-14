"""Teardown must remove this deployment's entries without touching other deployments."""

from pathlib import Path

import pytest
import tomlkit

from scripts import destroy
from scripts.mcp_setup import _write_codex_config
from scripts.setup_rtce import _write_codex_config_rtce


@pytest.mark.parametrize("matching", [True, False])
def test_codex_cleanup_uses_registered_environment(tmp_path, monkeypatch, matching):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    root = tmp_path / "project"
    root.mkdir()
    paths = [home / ".codex/config.toml", root / ".codex/config.toml"]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('# Keep this comment\nmodel = "existing-model"\n[mcp_servers.other]\ncommand = "other"\n')
        _write_codex_config(path, {"FLINK_ENV_ID": "env-demo", "KAFKA_ENV_ID": "env-demo"}, "npx")
        _write_codex_config_rtce(
            path, "confluent-fraud-agent-rtce",
            "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/org"
            "/environments/env-demo/kafka-clusters/lkc-demo", "test-token",
        )
    destroy._cleanup_codex_mcp(root, "env-demo" if matching else "env-other")
    for path in paths:
        text = path.read_text()
        doc = tomlkit.parse(text)
        assert doc["model"] == "existing-model"
        assert '# Keep this comment' in text
        assert doc["mcp_servers"]["other"]["command"] == "other"
        for name in ("confluent-cloud-mcp-server", "confluent-fraud-agent-rtce"):
            assert (name in doc["mcp_servers"]) is not matching


def test_codex_cleanup_runs_without_claude(tmp_path, monkeypatch):
    calls = []
    def missing_claude(*args, **kwargs):
        raise FileNotFoundError
    monkeypatch.setattr(destroy.subprocess, "run", missing_claude)
    monkeypatch.setattr(destroy, "_cleanup_codex_mcp", lambda root, env: calls.append((root, env)))
    destroy._cleanup_mcp(tmp_path, "env-demo")
    assert calls == [(tmp_path, "env-demo")]
