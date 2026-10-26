#!/usr/bin/env python3
"""
Deploy the real-time fraud-detection demo to Confluent Cloud.

Runs `terraform apply` in terraform/ (environment, cluster, Schema Registry,
Flink compute pool, keys, tools JAR, tables, the Bedrock connection only with
--bedrock, and — unless in workshop mode — the model, tools, agent, windowing,
and detection statements). The agent's model is CSP Hosted Flink Model Inference
by default (no AWS keys); --bedrock backs it with your own AWS Bedrock keys.
Terraform also writes the repo-root .env the producer and dashboard read.

Credentials live in credentials.env (gitignored): the first interactive run
prompts for them and saves them; later runs reuse them.

Usage:
  uv run deploy               # interactive; the only command a person needs
  uv run deploy --workshop    # workshop mode: participants build the AI pipeline by hand (WORKSHOP.md)
  uv run deploy --bedrock     # back the agent with AWS Bedrock (your own AWS keys) instead of CSP Hosted
  uv run deploy --automated   # bots: non-interactive from credentials.env, registers the MCP server with Claude Code
  uv run deploy --testing     # bots: non-interactive from credentials.env, no MCP registration
"""

import argparse
import getpass
import os
import subprocess
import sys
import time

from dotenv import dotenv_values, set_key

from scripts.credentials import generate_confluent_api_keys, load_or_create_credentials_file
from scripts.login_checks import (
    SSO_NOTE,
    _attempt_login_quiet,
    check_confluent_login,
    ensure_confluent_login,
)
from scripts.terraform import get_project_root, read_outputs, terraform_dir
from scripts.terraform_runner import run_terraform
from scripts.tfvars import (
    LLM_PROVIDER_KEY,
    LLM_PROVIDERS,
    is_workshop_mode,
    llm_provider_for,
    write_tfvars_for_deployment,
)
from scripts.ui import display_value, prompt_with_default

DEFAULT_REGION = "us-east-1"  # fixed in terraform/main.tf locals
DEFAULT_PREFIX = "fraud-agent"  # fixed in terraform/main.tf locals
REQUIRED_CREDENTIALS = {
    "TF_VAR_confluent_cloud_api_key": "Confluent Cloud API Key",
    "TF_VAR_confluent_cloud_api_secret": "Confluent Cloud API Secret",
}
BEDROCK_REQUIRED_CREDENTIALS = {
    "TF_VAR_aws_bedrock_access_key": "AWS Bedrock Access Key",
    "TF_VAR_aws_bedrock_secret_key": "AWS Bedrock Secret Key",
}

# Module-level buffer for Plan B fallback when set_key() fails (e.g. Windows locks).
_pending_writes: dict = {}
_write_failed: bool = False


def _detect_ambient_aws_credentials() -> tuple[str, str, str] | None:
    """Resolve AWS credentials via boto3's standard chain (env vars, `aws sso
    login` cache, shared config/profile, IMDS, ...). Returns
    (access_key, secret_key, session_token) — session_token is "" when the
    credentials aren't temporary — or None if nothing resolves.

    Preferred over pasting: a temporary session token is a long (~300-400 char)
    string, and pasting that into a plain terminal prompt can freeze some terminals.
    """
    try:
        import boto3

        resolved = boto3.Session().get_credentials()
        if resolved is None:
            return None
        frozen = resolved.get_frozen_credentials()
        if not frozen.access_key or not frozen.secret_key:
            return None
        return frozen.access_key, frozen.secret_key, frozen.token or ""
    except Exception:
        return None


def _save_env_safe(creds_file, key: str, value: str) -> None:
    """Write a key to credentials.env with retry + in-memory fallback."""
    global _write_failed
    if _write_failed:
        _pending_writes[key] = value
        return
    for attempt in range(3):
        result = set_key(str(creds_file), key, value)
        if result[0] is not None:
            check = dotenv_values(str(creds_file))
            if check.get(key) == value:
                return
        if attempt < 2:
            time.sleep(0.3 * (attempt + 1))
    _write_failed = True
    _pending_writes[key] = value


def _flush_pending_writes(creds_file) -> None:
    """Bulk-write any buffered credentials at the end of the flow (Plan B)."""
    if not _pending_writes:
        return
    try:
        existing = dotenv_values(str(creds_file)) if creds_file.exists() else {}
        existing.update(_pending_writes)
        with open(creds_file, "w", encoding="utf-8", newline="\n") as f:
            for k, v in existing.items():
                f.write(f"{k}='{v}'\n")
        check = dotenv_values(str(creds_file))
        for k, v in _pending_writes.items():
            if check.get(k) != v:
                raise ValueError(f"Read-back verification failed for {k}")
    except Exception as e:
        print(f"\n✗ Could not write credentials.env: {e}", file=sys.stderr)
        sys.exit(1)


def _deploy(root, creds: dict) -> None:
    print("\n=== Starting Deployment ===")
    if not write_tfvars_for_deployment(root, creds):
        print("Error: could not write terraform/terraform.tfvars (missing credentials?).")
        sys.exit(1)
    for key, value in creds.items():
        if value:
            os.environ[key] = value
    if not run_terraform(terraform_dir(root)):
        print("\nDeployment failed. Fix the error above and rerun `uv run deploy`.")
        sys.exit(1)
    print("\n✓ Deployment completed successfully!")


def _resolve_llm_provider(root, creds: dict, bedrock: bool) -> str:
    """--bedrock, else an exported TF_VAR_llm_provider, else llm_provider_for()'s state/saved rule.

    Exits cleanly on an unknown provider value.
    """
    requested = "bedrock" if bedrock else (os.environ.get(LLM_PROVIDER_KEY) or "").strip() or None
    try:
        return llm_provider_for(root, creds, requested)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


def _prompt_aws_credentials(creds_file, creds: dict) -> None:
    """The --bedrock flow: ambient AWS credentials first, else prompt; saves to credentials.env."""
    # AWS credentials power the Flink Bedrock connection the agent runs on. Try the
    # ambient AWS credential chain first (SSO login, env vars, a profile).
    print("\nAWS credentials power the Bedrock connection the fraud agent runs on (Claude in us-east-1).")
    access_key = secret_key = token = ""
    saved_key = creds.get("TF_VAR_aws_bedrock_access_key", "")
    ambient = None if saved_key else _detect_ambient_aws_credentials()
    if ambient:
        detected_key, detected_secret, detected_token = ambient
        masked = f"{detected_key[:4]}...{detected_key[-4:]}" if len(detected_key) > 8 else detected_key
        if input(f"Detected AWS credentials in your environment ({masked}). Use these? (Y/n): ").strip().lower() in (
            "",
            "y",
        ):
            access_key, secret_key, token = detected_key, detected_secret, detected_token
            if token:
                print(
                    "  Note: these are temporary credentials. The Bedrock connection stops working when they"
                    "\n  expire; use a long-lived IAM user key (`uv run api-keys create`) for a lasting demo."
                )

    if not access_key:
        access_key = prompt_with_default("AWS Access Key", saved_key)
        secret_key = prompt_with_default("AWS Secret Key", creds.get("TF_VAR_aws_bedrock_secret_key", ""), secret=True)
        if access_key.startswith("ASIA"):
            print(
                "Temporary credentials need a session token too — that's a long string"
                "\n(300-400 chars) that can freeze some terminals on paste. If it does,"
                "\nCtrl-C and re-run after `export AWS_SESSION_TOKEN=...` (and"
                "\nAWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) in your shell, so this can"
                "\nauto-detect it instead of needing it pasted here."
            )
            token = prompt_with_default(
                "AWS Session Token (temp creds)", creds.get("TF_VAR_aws_session_token", ""), secret=True
            )

    _save_env_safe(creds_file, "TF_VAR_aws_bedrock_access_key", access_key)
    _save_env_safe(creds_file, "TF_VAR_aws_bedrock_secret_key", secret_key)
    _save_env_safe(creds_file, "TF_VAR_aws_session_token", token)


def _check_bedrock(creds: dict) -> None:
    """Warn (never fail) when the AWS key can't invoke the demo's Claude model."""
    try:
        from scripts.test_bedrock_credentials import test_bedrock_credentials

        print("\nChecking the AWS key can invoke Claude on Bedrock...")
        ok, error_type = test_bedrock_credentials(
            creds.get("TF_VAR_aws_bedrock_access_key", ""),
            creds.get("TF_VAR_aws_bedrock_secret_key", ""),
            DEFAULT_REGION,
            session_token=creds.get("TF_VAR_aws_session_token") or None,
        )
    except Exception as e:
        print(f"⚠ Skipped the Bedrock check: {e}")
        return
    if ok:
        print("✓ Bedrock credentials work")
    else:
        print(
            f"⚠ Bedrock check failed ({error_type}). Deploy still runs, but the agent produces no alerts "
            "until the key can invoke Claude in us-east-1 (see README Prerequisites)."
        )


def _print_next_steps(root, workshop: bool) -> None:
    flink_url = "https://confluent.cloud/go/flink"  # generic fallback when outputs are unreadable
    try:
        outputs = read_outputs(root)
        print(f"\nEnvironment:     {outputs.get('confluent_environment_display_name', '?')}")
        print(f"Flink catalog:   {outputs.get('flink_catalog', '?')}")
        print(f"Flink database:  {outputs.get('flink_database', '?')}")
        env_id = outputs.get("confluent_environment_id")
        if env_id:
            flink_url = f"https://confluent.cloud/environments/{env_id}/flink"
            print(f"Flink SQL Workspace: {flink_url}")
    except Exception as e:
        print(f"\n⚠ Could not read Terraform outputs: {e}")
    print("\nNext:")
    if workshop:
        print(f"  Build the AI pipeline in the Flink workspace ({flink_url}) — follow WORKSHOP.md")
    print("  uv run producer     # stream synthetic events")
    print("  uv run dashboard    # live alerts at http://localhost:8501")
    print("  uv run setup-mcp    # let your coding agent run Flink SQL against this deployment")


def _start_producer(root) -> None:
    """Run the producer in the background, logging to tmp/producer.log."""
    log = root / "tmp" / "producer.log"
    log.parent.mkdir(exist_ok=True)
    with open(log, "a", encoding="utf-8") as out:
        subprocess.Popen(
            ["uv", "run", "producer"],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    print(f"✓ Producer streaming in the background (log: {log.relative_to(root)}; stop with `pkill -f producer`)")


def main():
    parser = argparse.ArgumentParser(description="Deploy the fraud-detection demo")
    parser.add_argument(
        "--workshop",
        action="store_true",
        help="Workshop mode: skip the AI pipeline statements participants create by hand (saved to credentials.env)",
    )
    parser.add_argument(
        "--automated",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, register the Confluent MCP server with Claude Code",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Bots: load credentials.env, skip prompts, no MCP registration",
    )
    parser.add_argument(
        "--bedrock",
        action="store_true",
        help="Back the fraud agent with AWS Bedrock (your own AWS keys) instead of CSP Hosted inference (saved)",
    )
    args = parser.parse_args()
    if args.automated and args.testing:
        parser.error("--automated and --testing are mutually exclusive")

    print("=== Fraud Detection Streaming Agent — Deploy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")
    creds_file = root / "credentials.env"

    # ---- Non-interactive -------------------------------------------------
    if args.automated or args.testing:
        if not creds_file.exists():
            print("Error: credentials.env not found. Run `uv run deploy` interactively first.")
            sys.exit(1)
        if args.workshop:
            set_key(str(creds_file), "TF_VAR_deploy_flink_pipeline", "false")
        llm_provider = _resolve_llm_provider(root, dotenv_values(str(creds_file)), args.bedrock)
        set_key(str(creds_file), LLM_PROVIDER_KEY, llm_provider)
        creds = dotenv_values(str(creds_file))
        required = {**REQUIRED_CREDENTIALS, **(BEDROCK_REQUIRED_CREDENTIALS if llm_provider == "bedrock" else {})}
        missing = [label for key, label in required.items() if not (creds.get(key) or "").strip()]
        if missing:
            print("Error: credentials.env is missing:")
            for m in missing:
                print(f"  - {m}")
            sys.exit(1)
        ensure_confluent_login(creds)
        workshop = is_workshop_mode(creds)
        print(
            f"✓ Credentials loaded. Region: {DEFAULT_REGION}. Mode: {'workshop' if workshop else 'demo'}. "
            f"LLM: {LLM_PROVIDERS[llm_provider]}\n"
        )
        _deploy(root, creds)
        if args.automated:
            from scripts.mcp_setup import main as setup_mcp

            setup_mcp(["--agent", "claude"])
        _print_next_steps(root, workshop)
        return

    # ---- Interactive -----------------------------------------------------
    creds_file, creds = load_or_create_credentials_file(root)

    # Confluent Cloud login (saved for auto-login).
    # (A live CLI session, e.g. from SSO, needs no prompt.)
    if not (creds.get("CONFLUENT_EMAIL") and creds.get("CONFLUENT_PASSWORD")) and not check_confluent_login():
        print("\nConfluent Cloud login (saved to credentials.env for auto-login):")
        print(f"  {SSO_NOTE}")
        for attempt in range(3):
            email = input("  Email (Enter if you use SSO or want to skip): ").strip()
            if not email:
                print("  Skipped. SSO users will be offered a single sign-on login next.")
                break
            password = getpass.getpass("  Password: ")
            if _attempt_login_quiet(email, password):
                _save_env_safe(creds_file, "CONFLUENT_EMAIL", email)
                _save_env_safe(creds_file, "CONFLUENT_PASSWORD", password)
                creds["CONFLUENT_EMAIL"] = email
                creds["CONFLUENT_PASSWORD"] = password
                print("  ✓ Logged in and saved.")
                break
            remaining = 2 - attempt
            print(
                f"  Login failed. {remaining} attempt(s) left."
                if remaining
                else "  Login failed. Skipping (use `confluent login --no-browser` if you use SSO)."
            )
    ensure_confluent_login(creds, interactive=True)
    print("✓ Confluent CLI logged in")
    print(f"\nRegion: {DEFAULT_REGION} (fixed)")

    # Optionally generate fresh Confluent Cloud API keys.
    if input("\nGenerate new Confluent Cloud API keys? (y/n): ").strip().lower() == "y":
        api_key, api_secret = generate_confluent_api_keys(DEFAULT_PREFIX)
        if api_key and api_secret:
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
            _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)
            creds["TF_VAR_confluent_cloud_api_key"] = api_key
            creds["TF_VAR_confluent_cloud_api_secret"] = api_secret

    print("\n--- Credential Configuration ---")
    api_key = prompt_with_default("Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", ""))
    api_secret = prompt_with_default(
        "Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""), secret=True
    )
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
    _save_env_safe(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

    llm_provider = _resolve_llm_provider(root, creds, args.bedrock)
    if llm_provider == "bedrock":
        _prompt_aws_credentials(creds_file, creds)
    else:
        print(f"\nLLM: {LLM_PROVIDERS[llm_provider]} — no AWS keys needed.")
        print("  (Re-run with --bedrock to back the agent with your own AWS Bedrock keys instead.)")
    _save_env_safe(creds_file, LLM_PROVIDER_KEY, llm_provider)
    if args.workshop:
        _save_env_safe(creds_file, "TF_VAR_deploy_flink_pipeline", "false")
    _flush_pending_writes(creds_file)

    final_creds = dotenv_values(creds_file)
    if llm_provider == "bedrock":
        _check_bedrock(final_creds)
    workshop = is_workshop_mode(final_creds)

    print("\n--- Configuration Summary ---")
    for key, value in sorted(final_creds.items()):
        if value:
            print(f"{key}: {display_value(key, value)}")
    print(f"\nRegion: {DEFAULT_REGION}")
    print(f"LLM: {LLM_PROVIDERS[llm_provider]}")
    if workshop:
        print(
            "Mode: WORKSHOP — infra, tables, and functions only; you build the AI pipeline by hand"
            "\n      (set TF_VAR_deploy_flink_pipeline=true in credentials.env to switch back)"
        )
    else:
        print("Mode: demo — deploys the full AI pipeline")

    if input("\nReady to deploy? (y/n): ").strip().lower() != "y":
        print("Deployment cancelled.")
        sys.exit(0)

    _deploy(root, final_creds)
    _print_next_steps(root, workshop)
    if workshop:
        return
    if input("\nStart the producer in the background and open the dashboard now? (Y/n): ").strip().lower() in (
        "",
        "y",
    ):
        _start_producer(root)
        from scripts.dashboard import main as run_dashboard

        run_dashboard([])


if __name__ == "__main__":
    main()
