#!/usr/bin/env python3
"""
Destroy the fraud-detection demo from Confluent Cloud.

Runs `terraform destroy` in terraform/ with the credentials in credentials.env,
then removes local Terraform state, the generated tfvars and .env, and the
Confluent MCP server registration. credentials.env is kept for the next deploy.

Usage:
  uv run destroy              # interactive confirmation
  uv run destroy --testing    # bots: non-interactive
  uv run destroy --force      # clean local state even if terraform destroy fails
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import tomlkit
from dotenv import dotenv_values

from .credentials import load_or_create_credentials_file
from .login_checks import ensure_confluent_login
from .mcp_setup import _codex_config_paths
from .terraform import get_project_root, read_outputs, terraform_dir
from .terraform_runner import run_terraform_destroy


def cleanup_terraform_artifacts(env_path: Path) -> None:
    """Remove state, generated tfvars, and provider caches after a successful destroy."""
    try:
        for path in [*env_path.glob("*.tfstate*"), *env_path.glob("*.tfvars"), *env_path.glob("*.tfvars.backup")]:
            path.unlink()
        if (env_path / ".terraform").exists():
            shutil.rmtree(env_path / ".terraform")
        lock_file = env_path / ".terraform.lock.hcl"
        if lock_file.exists():
            lock_file.unlink()
    except Exception:
        # Destroy succeeded; leftover local files are harmless.
        pass


def _cleanup_local_config(root: Path) -> None:
    """Remove the Terraform-written .env (its keys were just deleted)."""
    dotenv = root / ".env"
    if dotenv.exists():
        dotenv.unlink()
        print("✓ Removed .env")


def _cleanup_codex_mcp(root: Path, environment_id: str) -> None:
    """Remove only Codex entries targeting the destroyed environment, preserving other configs."""
    if not environment_id:
        return
    for config_path in _codex_config_paths(root):
        if not config_path.exists():
            continue
        try:
            doc = tomlkit.parse(config_path.read_text())
            servers = doc.get("mcp_servers", {})
            removed = []
            for name in ("confluent-cloud-mcp-server", "confluent-fraud-agent-rtce"):
                entry = servers.get(name, {})
                env = entry.get("env", {})
                url_path = urlsplit(entry.get("url", "")).path
                if environment_id in (env.get("FLINK_ENV_ID"), env.get("KAFKA_ENV_ID")) or (
                    f"/environments/{environment_id}/" in url_path
                ):
                    del servers[name]
                    removed.append(name)
            if removed:
                config_path.write_text(tomlkit.dumps(doc))
                for name in removed:
                    print(f"✓ Removed the {name} MCP server registration from {config_path}")
        except (OSError, tomlkit.exceptions.ParseError) as exc:
            print(f"⚠ Could not clean Codex MCP registrations in {config_path}: {type(exc).__name__}")


def _cleanup_mcp(root: Path, environment_id: str = "") -> None:
    """Remove the MCP server registrations `uv run setup-mcp` / `setup-rtce` added for this project."""
    for name in ("confluent-cloud-mcp-server", "confluent-fraud-agent-rtce"):
        try:
            result = subprocess.run(["claude", "mcp", "remove", name, "-s", "local"], capture_output=True, cwd=root)
        except FileNotFoundError:
            break
        if result.returncode == 0:
            print(f"✓ Removed the {name} MCP server registration")
    _cleanup_codex_mcp(root, environment_id)


def main():
    parser = argparse.ArgumentParser(description="Destroy the fraud-detection demo resources")
    parser.add_argument("--testing", action="store_true", help="Non-interactive: load credentials.env, skip prompts")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Clean local state even if terraform destroy fails (use when resources are already gone)",
    )
    args = parser.parse_args()

    print("=== Fraud Detection Streaming Agent — Destroy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")
    env_path = terraform_dir(root)

    if not (env_path / "terraform.tfstate").exists():
        print("⊘ Nothing to destroy: no terraform state found (never deployed)")
        return

    # Save the environment id before Terraform deletes its outputs and local state.
    environment_id = read_outputs(root).get("confluent_environment_id", "")

    if args.testing:
        creds_file = root / "credentials.env"
        if not creds_file.exists():
            print("Error: credentials.env not found.")
            sys.exit(1)
        creds = dotenv_values(str(creds_file))
    else:
        _, creds = load_or_create_credentials_file(root)
        ensure_confluent_login(creds, interactive=True)
        print("✓ Confluent CLI logged in")
        print("\n⚠️  WARNING: This permanently destroys the demo's Confluent Cloud environment and everything in it!")
        if input("\nAre you sure you want to proceed? (y/n): ").strip().lower() != "y":
            print("Destroy cancelled.")
            sys.exit(0)

    for key, value in creds.items():
        if value:
            os.environ[key] = value

    print("\n=== Starting Destroy ===")
    if run_terraform_destroy(env_path):
        cleanup_terraform_artifacts(env_path)
    elif args.force:
        print("  ⚠ Destroy failed but --force set: cleaning local state")
        cleanup_terraform_artifacts(env_path)
    else:
        print("\n✗ Destroy failed. Fix the error and rerun, or use --force to clean local state anyway.")
        sys.exit(1)

    _cleanup_local_config(root)
    _cleanup_mcp(root, environment_id)
    print("\n✓ Destroy process completed!")


if __name__ == "__main__":
    main()
