"""Help must be safe before deployment, with no Kafka connection configuration."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("command", ["producer", "datagen"])
def test_help_without_deployment(tmp_path, command):
    source = Path(__file__).resolve().parents[1] / "producer" / "generate_events.py"
    script = tmp_path / "producer" / "generate_events.py"
    script.parent.mkdir()
    script.write_text(source.read_text())
    env = {k: v for k, v in os.environ.items() if k not in {
        "BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET", "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET",
    }}
    result = subprocess.run(
        [sys.executable, "-c", f"import runpy, sys; sys.argv = [{command!r}, '--help']; "
         f"runpy.run_path({str(script)!r})['main']()"],
        env=env, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert f"usage: {command}" in result.stdout
    assert "Connecting" not in result.stdout
