from pathlib import Path
from unittest.mock import patch

import pytest

from scripts import login_checks as lc
from scripts import terraform_runner as tr


def test_interactive_offers_sso_login():
    with (
        patch.object(lc, "check_confluent_login", return_value=False),
        patch.object(lc, "attempt_confluent_auto_login", return_value=False),
        patch("sys.stdin.isatty", return_value=True),
        patch("builtins.input", return_value=""),
        patch.object(lc, "sso_login", return_value=True) as sso,
    ):
        lc.ensure_confluent_login({}, interactive=True)
    sso.assert_called_once()


def test_non_interactive_exits_with_no_browser_hint(capsys):
    with (
        patch.object(lc, "check_confluent_login", return_value=False),
        patch.object(lc, "attempt_confluent_auto_login", return_value=False),
        pytest.raises(SystemExit),
    ):
        lc.ensure_confluent_login({})
    assert "--no-browser" in capsys.readouterr().out


def test_dead_flink_key_detected_and_streamed():
    rc, out = tr._run_streaming(["sh", "-c", 'echo "Error: flink 401 Unauthorized"; exit 3'], Path("."))
    assert rc == 3 and tr._is_dead_flink_key(out)
    assert not tr._is_dead_flink_key("Error: 403 Forbidden")
