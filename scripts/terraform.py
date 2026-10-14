"""
Terraform helpers: locate the project root and the single Terraform root,
and read its outputs.

This demo has one Terraform root (`terraform/`), unlike the core + demo split
in the other streaming-agent repos, so every script reads one state file.
"""

import json
import logging
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """Find the project root (the directory holding pyproject.toml).

    Checks the current working directory and its parents first, then falls back
    to this file's location.
    """
    cwd = Path.cwd().resolve()
    for parent in [cwd, *cwd.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise FileNotFoundError("Could not find project root (pyproject.toml not found in any parent directory)")


def terraform_dir(root: Path | None = None) -> Path:
    """The demo's single Terraform root."""
    return (root or get_project_root()) / "terraform"


def state_path(root: Path | None = None) -> Path:
    return terraform_dir(root) / "terraform.tfstate"


def run_terraform_output(state: Path) -> dict[str, Any]:
    """Run `terraform output -json` against a state file and return {name: value}."""
    try:
        result = subprocess.run(
            ["terraform", "output", "-json", f"-state={state}"], capture_output=True, text=True, check=True
        )
        return {key: value["value"] for key, value in json.loads(result.stdout).items()}
    except FileNotFoundError:
        logger.error("Terraform binary not found. Install it: https://developer.hashicorp.com/terraform/install")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Terraform output failed: {e.stderr}")
        raise


def read_outputs(root: Path | None = None) -> dict[str, Any]:
    """Outputs of the deployed demo; raises FileNotFoundError when it isn't deployed."""
    state = state_path(root)
    if not state.exists():
        raise FileNotFoundError(f"{state} not found. Run `uv run deploy` first.")
    return run_terraform_output(state)
