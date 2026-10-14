"""
Register Confluent Cloud Real-Time Context Engine (RTCE) MCP server with a
local coding agent (Claude Code, Codex CLI, or Gemini CLI).

Prerequisites:
- RTCE is AWS-only. Azure deployments are not supported.
- A Global-scoped Confluent Cloud API key (--resource global). When deployed via
  `uv run deploy`, this key is created automatically via the Confluent CLI using the
  Terraform-provisioned RTCE service account (DeveloperRead on cluster + Schema Registry).
  For standalone use, create a key manually: Confluent Cloud UI → hamburger menu →
  API keys → Add API key → Global scope.
- Optionally, the script can enable RTCE on topics for you (requires topics to have a
  registered schema in Schema Registry).

Usage:
    uv run setup-rtce               # interactive: prompts for client(s), topics, etc.
    uv run setup-rtce --client codex
    uv run setup-rtce --dry-run     # print what would be registered, touch no agent config
    uv run setup-rtce --lightning fraud_alerts   # curl: scan a served table

Not used by the demo pipeline yet; kept so RTCE is one command away. Terraform
provisions the read-only RTCE service account (terraform/rtce.tf).
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_SCRIPT_DIR = Path(__file__).parent  # scripts/
_PROJECT_ROOT = _SCRIPT_DIR.parent  # repo root

try:
    from dotenv import dotenv_values, set_key

    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False

# Fixed, business-readable MCP server name. NOT suffixed with the per-deployment
# random id on purpose: MCP allowlists (e.g. org-managed Claude Code policies) match
# the name exactly, so a suffix would un-allowlist the server on every redeploy.
# Re-registering with the same name simply overwrites the prior entry, which is what a
# teardown+redeploy wants. So we never prompt for it.
_DEFAULT_SERVER_NAME = "confluent-fraud-agent-rtce"
_CRED_KEY = "CONFLUENT_RTCE_API_KEY"
_CRED_SECRET = "CONFLUENT_RTCE_API_SECRET"
_CRED_TOPICS = "CONFLUENT_RTCE_TOPICS"
# Name of the Flink MCP connection to RTCE that create_agent_connection() makes, for a
# Streaming Agent tool that reads live context (none uses it yet).
AGENT_CONNECTION = "rtce-connection"

# Terraform output key names from terraform/ state.
_TF_ORG = "confluent_organization_id"
_TF_ENV = "confluent_environment_id"
_TF_CLUSTER = "confluent_kafka_cluster_id"
_TF_REGION = "cloud_region"
_TF_CLOUD = "cloud_provider"
_TF_RTCE_SA_ID = "confluent_rtce_service_account_id"

# AWS regions where RTCE is currently available.
# Source: https://docs.confluent.io/cloud/current/clusters/regions.html
# New regions are added regularly — warn but don't block if a region isn't listed.
_RTCE_SUPPORTED_AWS_REGIONS = {
    "us-east-1",
    "us-east-2",
    "us-west-2",
    "ap-northeast-2",
    "ap-south-1",
    "ap-southeast-1",
    "ap-southeast-2",
}


def _load_env_file(path: Path) -> dict:
    if not path.exists():
        return {}
    if _DOTENV_AVAILABLE:
        return dict(dotenv_values(path))
    # Minimal fallback parser for KEY=VALUE lines.
    result = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        result[k.strip()] = v.strip().strip('"').strip("'")
    return result


def _save_to_env_file(path: Path, key: str, value: str) -> None:
    if _DOTENV_AVAILABLE:
        set_key(str(path), key, value)
        return
    # Minimal fallback: append if not present, replace if present.
    lines = path.read_text().splitlines() if path.exists() else []
    new_lines = []
    found = False
    for line in lines:
        if line.startswith(f"{key}=") or line.startswith(f"{key} ="):
            new_lines.append(f'{key}="{value}"')
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f'{key}="{value}"')
    path.write_text("\n".join(new_lines) + "\n")


def _find_credentials_file() -> Path:
    """Look for credentials.env starting from cwd upward, then default to cwd."""
    here = Path.cwd()
    for parent in [here, *here.parents]:
        candidate = parent / "credentials.env"
        if candidate.exists():
            return candidate
    return here / "credentials.env"


def _read_core_tf_outputs(creds_file: Path) -> dict:
    """Return terraform/ outputs dict, or empty dict if state is not found."""
    for root in [creds_file.parent, Path.cwd()]:
        state_path = root / "terraform" / "terraform.tfstate"
        if state_path.exists():
            return _read_terraform_outputs(state_path)
    return {}


def _discover_demo_topics(creds_file: Path) -> list[str]:
    """The demo's topics from terraform/ state (source + served), in a
    stable order. Empty list if the state isn't found. This is the SSOT that lets
    setup-rtce pick the topic set itself instead of asking the user."""
    for root in [creds_file.parent, Path.cwd()]:
        state_path = root / "terraform" / "terraform.tfstate"
        if state_path.exists():
            outputs = _read_terraform_outputs(state_path)
            source = outputs.get("source_topics") or []
            served = outputs.get("served_topics") or []
            # de-dupe while preserving order (sources first, then served)
            seen: dict[str, None] = {}
            for t in [*source, *served]:
                if isinstance(t, str):
                    seen.setdefault(t, None)
            return list(seen)
    return []


def _create_rtce_global_key_via_cli(sa_id: str) -> tuple[str, str]:
    """Create a Global API key for the RTCE service account via Confluent CLI.

    Uses `confluent api-key create --resource global` which is the only way to create
    a Global-scoped key (the Terraform provider creates Cloud/CRM keys, not Global ones).
    Returns (api_key, api_secret) or ("", "") on failure.
    """
    result = subprocess.run(
        [
            "confluent",
            "api-key",
            "create",
            "--resource",
            "global",
            "--service-account",
            sa_id,
            "--description",
            "Global API Key for RTCE MCP server (auto-created by setup-rtce)",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        if result.stderr:
            print(f"  CLI error: {result.stderr.strip()[:300]}")
        return "", ""

    api_key = api_secret = ""
    for line in result.stdout.split("\n"):
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 2:
            continue
        if parts[0] == "API Key":
            api_key = parts[1]
        elif parts[0] == "API Secret":
            api_secret = parts[1]
    return api_key, api_secret


def _key_owned_by(sa_id: str, api_key: str) -> bool:
    """True when api_key is one of sa_id's Global keys. A teardown deletes the service
    account and its keys, but credentials.env keeps the old key."""
    result = subprocess.run(
        ["confluent", "api-key", "list", "--resource", "global", "--service-account", sa_id, "-o", "json"],
        capture_output=True, text=True, timeout=60,
    )
    try:
        return any(k.get("key") == api_key for k in json.loads(result.stdout or "[]"))
    except (json.JSONDecodeError, AttributeError):
        return True  # can't tell; keep the stored key and let the MCP check report it


def _get_credentials(creds_file: Path, creds: dict) -> tuple[str, str]:
    api_key = creds.get(_CRED_KEY, "").strip()
    api_secret = creds.get(_CRED_SECRET, "").strip()

    if api_key and api_secret:
        print(f"Using RTCE API key from {creds_file.name}: {api_key[:8]}...")
        return api_key, api_secret

    print("\nNo RTCE Global API key found in credentials.env.")
    print(
        "For standalone use: create a key in the Confluent Cloud UI → hamburger → API keys → Global scope."
    )
    print()
    api_key = _ask("Global API key:    ")
    api_secret = _ask("Global API secret: ")

    _save_to_env_file(creds_file, _CRED_KEY, api_key)
    _save_to_env_file(creds_file, _CRED_SECRET, api_secret)
    print(f"✓ Saved credentials to {creds_file}")

    return api_key, api_secret


def _mcp_url(infra: dict[str, str]) -> str:
    return (
        f"https://mcp.{infra['region']}.aws.confluent.cloud/mcp/v1/context-engine"
        f"/organizations/{infra['org_id']}"
        f"/environments/{infra['env_id']}"
        f"/kafka-clusters/{infra['cluster_id']}"
    )


def core_infra(creds_file: Path) -> dict[str, str] | None:
    """org, environment, cluster, and region from terraform/ state, or None before it's deployed."""
    outputs = _read_core_tf_outputs(creds_file)
    infra = {
        "org_id": str(outputs.get(_TF_ORG, "")).strip(),
        "env_id": str(outputs.get(_TF_ENV, "")).strip(),
        "cluster_id": str(outputs.get(_TF_CLUSTER, "")).strip(),
        "region": str(outputs.get(_TF_REGION, "")).strip(),
    }
    return infra if all(infra.values()) else None


def _flink_scope(infra: dict[str, str]) -> list[str]:
    return ["--cloud", "aws", "--region", infra["region"], "--environment", infra["env_id"]]


def agent_connection_exists(infra: dict[str, str]) -> bool:
    result = subprocess.run(
        ["confluent", "flink", "connection", "describe", AGENT_CONNECTION, *_flink_scope(infra)],
        capture_output=True, text=True, timeout=60,
    )
    return result.returncode == 0


def create_agent_connection(infra: dict[str, str], api_key: str, api_secret: str) -> bool:
    """Create the recovery agent's MCP connection to RTCE, or refresh its key if it exists.

    The CLI, not Terraform, because the provider can't set the streamable HTTP transport
    RTCE needs. The Global key is Basic auth (username = key, password = secret).
    """
    if agent_connection_exists(infra):
        result = subprocess.run(
            ["confluent", "flink", "connection", "update", AGENT_CONNECTION, *_flink_scope(infra),
             "--username", api_key, "--password", api_secret, "--transport-type", "STREAMABLE_HTTP"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0 and result.stderr:
            print(f"  CLI error: {result.stderr.strip()[:300]}")
        return result.returncode == 0
    result = subprocess.run(
        ["confluent", "flink", "connection", "create", AGENT_CONNECTION, *_flink_scope(infra),
         "--type", "mcp_server", "--endpoint", _mcp_url(infra),
         "--username", api_key, "--password", api_secret, "--transport-type", "STREAMABLE_HTTP"],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0 and result.stderr:
        print(f"  CLI error: {result.stderr.strip()[:300]}")
    return result.returncode == 0


def _read_terraform_outputs(state_path: Path) -> dict:
    try:
        state = json.loads(state_path.read_text())
        return {k: v["value"] for k, v in state.get("outputs", {}).items()}
    except Exception:
        return {}


def _get_infra(creds_file: Path) -> dict[str, str]:
    """Return org_id, env_id, cluster_id, region — from terraform state or prompts."""
    # Try to find terraform state relative to the credentials file or cwd.
    search_roots = [creds_file.parent, Path.cwd()]
    outputs: dict = {}
    for root in search_roots:
        state_path = root / "terraform" / "terraform.tfstate"
        if state_path.exists():
            outputs = _read_terraform_outputs(state_path)
            break

    org_id = outputs.get(_TF_ORG, "").strip()
    env_id = outputs.get(_TF_ENV, "").strip()
    cluster_id = outputs.get(_TF_CLUSTER, "").strip()
    region = outputs.get(_TF_REGION, "").strip()
    cloud = outputs.get(_TF_CLOUD, "").strip().lower()

    if org_id and env_id and cluster_id and region:
        print("\nAuto-detected infrastructure from terraform state:")
        print(f"  Cloud:        {cloud or 'unknown'}")
        print(f"  Organization: {org_id}")
        print(f"  Environment:  {env_id}")
        print(f"  Cluster:      {cluster_id}")
        print(f"  Region:       {region}")
        return {
            "cloud": cloud,
            "org_id": org_id,
            "env_id": env_id,
            "cluster_id": cluster_id,
            "region": region,
        }

    print("\nEnter your Confluent Cloud infrastructure details:")
    if not cloud:
        raw_cloud = input("  Cloud provider (aws) [aws]: ").strip().lower()
        cloud = raw_cloud if raw_cloud in ("aws",) else "aws"
    if not org_id:
        org_id = _ask("  Organization ID (e.g. 5a49b747-3e48-4fbb-b679-cfaa8ebd5fee): ")
    if not env_id:
        env_id = _ask("  Environment ID (e.g. env-abc123): ")
    if not cluster_id:
        cluster_id = _ask("  Kafka Cluster ID (e.g. lkc-abc123): ")
    if not region:
        region = _ask("  AWS Region (e.g. us-east-1): ")

    return {
        "cloud": cloud,
        "org_id": org_id,
        "env_id": env_id,
        "cluster_id": cluster_id,
        "region": region,
    }


def _validate_rtce_support(infra: dict[str, str]) -> None:
    """Gate on cloud/region support. Hard-stops non-AWS; warns on unknown AWS regions."""
    cloud = infra.get("cloud", "aws").lower()
    region = infra.get("region", "")

    if cloud != "aws":
        print()
        print("=" * 70)
        print(f"ERROR: RTCE is not supported on '{cloud}'.")
        print()
        print("Real-Time Context Engine is currently an AWS-only feature.")
        print("=" * 70)
        sys.exit(1)

    if region not in _RTCE_SUPPORTED_AWS_REGIONS:
        print()
        print("!" * 70)
        print(
            f"WARNING: '{region}' is not in the known list of RTCE-supported AWS regions."
        )
        print()
        print("Known supported regions:")
        for r in sorted(_RTCE_SUPPORTED_AWS_REGIONS):
            print(f"  {r}")
        print()
        print("This probably will not work UNLESS Confluent has already rolled out")
        print("RTCE support for your region since this script was last updated.")
        print("Check https://docs.confluent.io/cloud/current/clusters/regions.html")
        print("for the current list.")
        print("!" * 70)
        raw = input("\nContinue anyway? [y/N]: ").strip().lower()
        if raw != "y":
            sys.exit(0)


def _list_rtce_topics(
    api_key: str, api_secret: str, infra: dict[str, str]
) -> list[str]:
    """Return RTCE-enabled topic names. Tries Confluent CLI first, then REST API.

    The CLI uses the logged-in user session (always has correct permissions).
    The REST API requires the Global API key to have CloudClusterAdmin on the
    cluster, which may not be the case — and it silently returns an empty list
    rather than a 403 when the key lacks that role.
    """
    env_id = infra["env_id"]
    cluster_id = infra["cluster_id"]

    # CLI path — reliable when user is logged in via `confluent login`.
    try:
        result = subprocess.run(
            [
                "confluent",
                "rtce",
                "rtce-topic",
                "list",
                "--cluster",
                cluster_id,
                "--environment",
                env_id,
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            items = json.loads(result.stdout or "[]")
            return [i["topic_name"] for i in items if i.get("phase") != "DISABLED"]
    except Exception:
        pass

    # REST API fallback (requires CloudClusterAdmin on the Global API key).
    url = (
        f"https://api.confluent.cloud/rtce/v1/rtce-topics"
        f"?environment={env_id}&spec.kafka_cluster={cluster_id}"
    )
    token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return [item["spec"]["topic_name"] for item in data.get("data", [])]
    except Exception as exc:
        print(f"  Warning: could not list RTCE-enabled topics ({exc})")
    return []


def _enable_rtce_topic(
    api_key: str,
    api_secret: str,
    infra: dict[str, str],
    topic_name: str,
    description: str,
) -> bool:
    """Enable RTCE on a topic. Tries Confluent CLI first, then REST API."""
    # CLI path — uses the logged-in session; avoids REST API permission issues.
    try:
        result = subprocess.run(
            [
                "confluent",
                "rtce",
                "rtce-topic",
                "create",
                "--cloud",
                infra["cloud"].lower(),
                "--region",
                infra["region"],
                "--topic-name",
                topic_name,
                "--description",
                description,
                "--cluster",
                infra["cluster_id"],
                "--environment",
                infra["env_id"],
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            return True
        stderr = result.stderr.strip()
        if "already exists" in stderr.lower() or "conflict" in stderr.lower():
            return True
        if stderr:
            print(f"  CLI error: {stderr[:300]}")
        # Fall through to REST API on CLI failure.
    except Exception:
        pass

    # REST API fallback (requires CloudClusterAdmin on the Global API key).
    url = "https://api.confluent.cloud/rtce/v1/rtce-topics"
    token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    body = json.dumps(
        {
            "spec": {
                "cloud": infra["cloud"].upper(),
                "region": infra["region"],
                "topic_name": topic_name,
                "description": description,
                "environment": {"id": infra["env_id"]},
                "kafka_cluster": {"id": infra["cluster_id"]},
            }
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status in (200, 201, 202)
    except urllib.error.HTTPError as exc:
        if exc.code == 409:  # already enabled
            return True
        err_body = exc.read().decode(errors="replace")
        print(f"  REST error {exc.code}: {err_body[:300]}")
        return False
    except Exception as exc:
        print(f"  Error: {exc}")
        return False


def _ask(prompt: str) -> str:
    """Prompt until the user provides a non-empty value (guards against buffered newlines)."""
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("  (Value required — please try again)")


def _pick_client() -> str:
    """Ask which MCP client to register with. Returns 'claude', 'codex', 'gemini', or 'none'."""
    print()
    print("Which AI assistant should the RTCE MCP server be registered with?")
    print("  1. Claude Code (default)")
    print("  2. OpenAI Codex")
    print("  3. Gemini")
    print("  4. None (the app still works)")
    raw = input("Enter 1, 2, 3, or 4 [1]: ").strip()
    return {"2": "codex", "3": "gemini", "4": "none"}.get(raw, "claude")


def _claude_argv_rtce(
    server_name: str, url: str, token: str
) -> tuple[list[str], list[str]]:
    """(remove, add) argv for Claude Code.

    `remove` uses `-s local`; `add` lets the scope default to local (no `--scope`).
    """
    return (
        ["claude", "mcp", "remove", server_name, "-s", "local"],
        [
            "claude", "mcp", "add", "--transport", "http", server_name, url,
            "--header", f"Authorization: Basic {token}",
        ],
    )


def _register_with_claude_code(
    server_name: str, url: str, token: str, dry_run: bool = False
) -> str:
    """Register RTCE as an HTTP MCP server in Claude Code.

    Primary path runs `claude mcp add --transport http`. Falls back to writing
    ~/.claude.json directly when the `claude` binary is absent or the CLI call fails
    (e.g. a managed-device policy blocks it). The direct write produces a byte-identical entry, so either
    path lands the same config.
    """
    remove_argv, add_argv = _claude_argv_rtce(server_name, url, token)

    if dry_run:
        redacted = [
            "Authorization: Basic <redacted>" if c.startswith("Authorization:") else c
            for c in add_argv
        ]
        print("[dry-run] Claude Code — would run (CLI primary):")
        print(f"    {shlex.join(remove_argv)}")
        print(f"    {shlex.join(redacted)}")
        print(f"    (fallback if `claude` is unavailable/blocked: write {Path.home() / '.claude.json'})")
        return "Claude Code local scope (http) [dry-run]"

    try:
        subprocess.run(remove_argv, capture_output=True)
        result = subprocess.run(add_argv, capture_output=True, text=True)
        if result.returncode == 0:
            return "Claude Code local scope (http, via claude CLI)"
        detail = result.stderr.strip() or f"exit {result.returncode}"
        print(f"  `claude mcp add` failed ({detail}); writing ~/.claude.json directly instead.")
    except FileNotFoundError:
        print("  `claude` CLI not found; writing ~/.claude.json directly instead.")

    return _register_with_claude_code_filewrite(server_name, url, token)


def _register_with_claude_code_filewrite(
    server_name: str, url: str, token: str
) -> str:
    """Fallback: write the RTCE HTTP MCP entry straight into ~/.claude.json.

    Works whether or not the `claude` binary is allowlisted, and produces the same
    projects[project_key].mcpServers[server_name] entry the CLI would.

    Literal token: Claude Code does not auto-load credentials.env into its
    environment, so an env-var placeholder ("Basic ${...}") would silently fail to
    authenticate unless the user exports it in the launching shell. Writing the
    resolved token here mirrors how mcp_setup.py stores secrets in this file.
    """
    project_key = str(_PROJECT_ROOT)
    claude_json_path = Path.home() / ".claude.json"

    claude_data: dict = {}
    if claude_json_path.exists():
        try:
            claude_data = json.loads(claude_json_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    entry = {
        "type": "http",
        "url": url,
        "headers": {"Authorization": f"Basic {token}"},
    }

    (
        claude_data.setdefault("projects", {})
        .setdefault(project_key, {})
        .setdefault("mcpServers", {})
    )[server_name] = entry

    claude_json_path.write_text(json.dumps(claude_data, indent=2) + "\n")
    return "Claude Code local scope (http, direct write)"


def _codex_config_paths_rtce() -> list[Path]:
    """Return Codex config files that can affect this project (home + project-local)."""
    home_config_path = Path.home() / ".codex" / "config.toml"
    paths = [home_config_path]
    project_config_path = Path.cwd() / ".codex" / "config.toml"
    if project_config_path.exists() and (
        project_config_path.resolve() != home_config_path.resolve()
    ):
        paths.append(project_config_path)
    return paths


def _write_codex_config_rtce(
    config_path: Path, server_name: str, url: str, token: str, dry_run: bool = False
) -> None:
    """Merge an `[mcp_servers.<name>]` table into one Codex config.toml file, in place.

    Uses tomlkit so round-tripping preserves comments/formatting of the rest of the
    file. There is no Codex CLI flag that expresses an arbitrary Basic-auth
    header (only `--bearer-token-env-var`, and RTCE requires Basic, not Bearer) — so
    this edits the TOML directly instead of shelling out to `codex mcp add`.
    """
    import tomlkit

    if dry_run:
        print(f"[dry-run] Codex CLI — would merge into {config_path}:\n")
        print(f'[mcp_servers.{server_name}]')
        print(f'url = "{url}"')
        print('http_headers = { "Authorization" = "Basic <redacted>" }')
        return

    try:
        existing_text = config_path.read_text() if config_path.exists() else ""
        doc = tomlkit.parse(existing_text) if existing_text else tomlkit.document()

        mcp_servers = doc.get("mcp_servers")
        if mcp_servers is None:
            mcp_servers = tomlkit.table()
            doc["mcp_servers"] = mcp_servers

        entry = tomlkit.table()
        entry["url"] = url
        http_headers = tomlkit.inline_table()
        http_headers["Authorization"] = f"Basic {token}"
        entry["http_headers"] = http_headers
        mcp_servers[server_name] = entry

        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(tomlkit.dumps(doc))
    except Exception as exc:
        print(f"  Warning: could not auto-edit {config_path} ({exc}); add this by hand:")
        print(f'[mcp_servers.{server_name}]')
        print(f'url = "{url}"')
        print(f'http_headers = {{ "Authorization" = "Basic {token}" }}')


def _register_with_codex_rtce(
    server_name: str, url: str, token: str, dry_run: bool = False
) -> str:
    """Write the RTCE HTTP MCP server into Codex config. Returns a scope note."""
    config_paths = _codex_config_paths_rtce()
    for codex_config_path in config_paths:
        _write_codex_config_rtce(codex_config_path, server_name, url, token, dry_run=dry_run)

    suffix = " [dry-run]" if dry_run else ""
    scope_note = f"Codex ({config_paths[0]}){suffix}"
    for shadow_path in config_paths[1:]:
        print(f"✓ Updated project-local Codex config at {shadow_path}")
    return scope_note


def _register_gemini(server_name: str, url: str, token: str, dry_run: bool = False) -> str:
    """Register the RTCE MCP server with Gemini CLI. Returns a scope note."""
    cmd = [
        "gemini",
        "mcp",
        "add",
        "--transport",
        "http",
        "--scope",
        "user",
        "--header",
        f"Authorization: Basic {token}",
        server_name,
        url,
    ]
    if dry_run:
        print("[dry-run] Gemini CLI — would run:")
        print(f"    gemini mcp remove {server_name}")
        redacted = [c if not c.startswith("Authorization:") else "Authorization: Basic <redacted>" for c in cmd]
        print(f"    {shlex.join(redacted)}")
        return "gemini user scope [dry-run]"

    subprocess.run(
        ["gemini", "mcp", "remove", server_name],
        capture_output=True,
    )
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error registering with Gemini: {result.stderr.strip()}")
        sys.exit(1)
    return "gemini user scope"


def _test_mcp_connection(url: str, token: str) -> tuple[bool, int]:
    """Send an MCP initialize POST to verify the Global API key works with the endpoint."""
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "setup-rtce", "version": "0.1"},
            },
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, resp.status
    except urllib.error.HTTPError as exc:
        return False, exc.code
    except Exception:
        return False, 0


def _parse_topics(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(",") if t.strip()]


_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_LIGHTNING_OPS = {"=", "<", "<=", ">", ">=", "IN", "LIKE"}
# Every Lightning query returns at most this many rows, whatever LIMIT says.
LIGHTNING_MAX_ROWS = 200


def _lightning_identifier(name: str) -> str:
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid Lightning identifier: {name!r}")
    return '"KEY"' if name == "key" else name


def _lightning_literal(value) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Unsupported Lightning value: {value!r}")
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Unsupported Lightning value: {value!r}")
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError(f"Unsupported Lightning value: {value!r}")
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S}'"
    return "'" + str(value).replace("'", "''") + "'"


def _build_lightning_query(
    topic: str,
    key: str | None = None,
    limit: int = 10,
    where=(),
    order_by: tuple[str, str] | None = None,
) -> str:
    """Build the SELECT a Lightning Query runs.

    `where` is a sequence of (column, op, value) conditions joined with AND; op is one
    of =, <, <=, >, >=, IN (value is a list/tuple) or LIKE. `key` is shorthand for
    ("key", "=", key). `order_by` is (column, "ASC"|"DESC"). Table and column names
    must be lowercase identifiers; the table is backtick-quoted and the key column is
    written "KEY", matching what the Lightning SQL surface accepts. Strings are
    single-quoted with embedded quotes doubled, numbers are unquoted, and datetimes
    become TIMESTAMP literals (UTC). Anything else raises ValueError.

    Lightning caps every result at 200 rows and rejects OFFSET, COUNT and GROUP BY,
    so callers page with `"KEY" > last ORDER BY "KEY"` instead.
    """
    if not isinstance(topic, str) or not _IDENTIFIER.fullmatch(topic):
        raise ValueError(f"Invalid Lightning table: {topic!r}")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError(f"Invalid Lightning limit: {limit!r}")
    conditions = [("key", "=", key)] if key is not None else []
    conditions.extend(where)
    clauses = []
    for condition in conditions:
        try:
            column, op, value = condition
        except (TypeError, ValueError):
            raise ValueError(f"Invalid Lightning condition: {condition!r}") from None
        name = _lightning_identifier(column)
        op = op.upper() if isinstance(op, str) else op
        if op not in _LIGHTNING_OPS:
            raise ValueError(f"Unsupported Lightning operator: {op!r}")
        if op == "IN":
            if not isinstance(value, (list, tuple)) or not value:
                raise ValueError("IN needs a non-empty list or tuple")
            rendered = "(" + ", ".join(_lightning_literal(v) for v in value) + ")"
        else:
            rendered = _lightning_literal(value)
        clauses.append(f"{name} {op} {rendered}")
    query = f"SELECT * FROM `{topic}`"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    if order_by is not None:
        try:
            column, direction = order_by
        except (TypeError, ValueError):
            raise ValueError(f"Invalid Lightning order: {order_by!r}") from None
        direction = direction.upper() if isinstance(direction, str) else direction
        if direction not in {"ASC", "DESC"}:
            raise ValueError(f"Invalid Lightning order direction: {direction!r}")
        query += f" ORDER BY {_lightning_identifier(column)} {direction}"
    return f"{query} LIMIT {limit}"


def lightning_command(
    infra: dict[str, str],
    api_key: str,
    api_secret: str,
    topic: str,
    key: str | None = None,
    limit: int = 10,
) -> str:
    """Build a shell-safe Lightning Queries curl command against this deployment.

    Lightning Queries share the same Global API key + Basic-auth pattern as RTCE, but
    hit the SQL query endpoint directly instead of going through the MCP protocol —
    useful for showing "an app hitting current state directly" without an agent hop.
    """
    region = infra.get("region", "")
    cloud = infra.get("cloud", "aws").lower()
    org_id = infra.get("org_id", "")
    env_id = infra.get("env_id", "")
    cluster_id = infra.get("cluster_id", "")
    missing = [
        name
        for name, value in (
            ("region", region),
            ("org_id", org_id),
            ("env_id", env_id),
            ("cluster_id", cluster_id),
        )
        if not value
    ]
    if missing or not api_key or not api_secret:
        raise ValueError(
            "Missing infrastructure details for Lightning Queries: "
            + ", ".join(missing or ["API key/secret"])
        )
    url = f"https://sql.{region}.{cloud}.confluent.cloud/query/v1alpha1"
    payload = json.dumps(
        {
            "catalog_name": env_id,
            "database_name": cluster_id,
            "query": _build_lightning_query(topic, key=key, limit=limit),
        },
        indent=2,
    )
    token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    return " \\\n  ".join(
        (
            "curl -sS -X POST " + shlex.quote(url),
            "-H " + shlex.quote("Authorization: Basic " + token),
            "-H " + shlex.quote("Content-Type: application/json"),
            "-d " + shlex.quote(payload),
        )
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="uv run setup-rtce",
        description=(
            "Register Confluent Cloud Real-Time Context Engine as an MCP server "
            "with a local coding agent."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--client",
        choices=("claude", "codex", "gemini", "none"),
        default=None,
        help="which coding agent to register with (default: prompt interactively)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be registered/enabled, but change no agent config or Confluent Cloud state",
    )
    parser.add_argument(
        "--lightning",
        metavar="TOPIC",
        help="print a ready-to-run Lightning Queries curl command for TOPIC; do not configure an MCP client",
    )
    parser.add_argument(
        "--key",
        metavar="KEY",
        help="with --lightning: filter the query to one row by its KEY",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        metavar="N",
        help="with --lightning: max rows to return (default: 10)",
    )
    args = parser.parse_args(argv)
    if args.lightning and (args.client or args.dry_run):
        parser.error("--lightning cannot be combined with --client or --dry-run")
    if (args.key or args.limit != 10) and not args.lightning:
        parser.error("--key/--limit only apply with --lightning")
    if args.limit < 1:
        parser.error("--limit must be >= 1")
    return args


def main(argv: list[str] | None = None):
    args = _parse_args(argv)

    creds_file = _find_credentials_file()
    creds = _load_env_file(creds_file)

    # Auto-create a Global API key when none is stored in credentials.env, or when the
    # stored one belongs to a service account from an earlier deployment.
    # Terraform provisions the service account + role bindings; this script creates
    # the Global key (--resource global) via CLI, which the Terraform provider can't do.
    sa_id = _read_core_tf_outputs(creds_file).get(_TF_RTCE_SA_ID, "").strip()
    stored = creds.get(_CRED_KEY, "").strip()
    if stored and sa_id and not args.dry_run and not _key_owned_by(sa_id, stored):
        print(f"Stored RTCE key {stored[:8]}... is not a key of {sa_id} (earlier deployment).")
        creds[_CRED_KEY] = creds[_CRED_SECRET] = ""
    if not creds.get(_CRED_KEY) and not args.dry_run:
        if sa_id:
            print(
                f"Creating Global API key for RTCE service account {sa_id} via CLI..."
            )
            key, secret = _create_rtce_global_key_via_cli(sa_id)
            if key and secret:
                print(f"  ✓ Created Global API key: {key[:8]}...")
                creds[_CRED_KEY] = key
                creds[_CRED_SECRET] = secret
                _save_to_env_file(creds_file, _CRED_KEY, key)
                _save_to_env_file(creds_file, _CRED_SECRET, secret)
            else:
                print(
                    "  ✗ CLI key creation failed — check that `confluent login` is active."
                )

    api_key, api_secret = _get_credentials(creds_file, creds)
    infra = _get_infra(creds_file)
    _validate_rtce_support(infra)

    if args.lightning:
        try:
            print(
                lightning_command(
                    infra, api_key, api_secret, args.lightning, args.key, args.limit
                )
            )
        except ValueError as exc:
            sys.exit(str(exc))
        return

    # We name the MCP server ourselves — no prompt. See _DEFAULT_SERVER_NAME.
    server_name = _DEFAULT_SERVER_NAME

    client = args.client if args.client else _pick_client()

    print("\nFetching currently RTCE-enabled topics...")
    already_enabled = _list_rtce_topics(api_key, api_secret, infra)
    if already_enabled:
        print(f"  Already RTCE-enabled: {', '.join(already_enabled)}")
    else:
        print("  No topics are currently RTCE-enabled on this cluster.")

    # Pick the topic set ourselves — never ask. Prefer the demo's own topics from
    # terraform state (source + served); union in anything already RTCE-enabled so a
    # hand-enabled topic still gets connected. Fall back to the last saved set, then
    # to whatever is already enabled. Only prompt as a last resort (no state at all).
    demo_topics = _discover_demo_topics(creds_file)
    saved_topics = _parse_topics(creds.get(_CRED_TOPICS, ""))
    base = demo_topics or saved_topics or already_enabled
    # de-dupe (base first, then any extra already-enabled topics), preserving order
    topics = list(dict.fromkeys([*base, *already_enabled]))
    if topics:
        origin = "terraform state" if demo_topics else (creds_file.name if saved_topics else "already-enabled")
        print(f"\nConnecting all demo topics (from {origin}): {', '.join(topics)}")
    else:
        print()
        topics = _parse_topics(_ask("Topics to connect to this MCP server (comma-separated): "))

    # Enable RTCE on any topic not yet enabled — automatically, no prompt. RTCE needs
    # a registered SR schema; every demo topic has one (created at deploy time).
    not_yet_enabled = [t for t in topics if t not in already_enabled]
    if not_yet_enabled and args.dry_run:
        print(f"[dry-run] Would enable RTCE on: {', '.join(not_yet_enabled)}")
    elif not_yet_enabled:
        print("\nEnabling RTCE on topics not yet enabled:")
        for topic in not_yet_enabled:
            desc = f"Data from the {topic} Kafka topic"
            print(f"  • {topic}...", end=" ", flush=True)
            ok = _enable_rtce_topic(api_key, api_secret, infra, topic, desc)
            print("✓" if ok else "✗  (see error above)")

    if not args.dry_run:
        _save_to_env_file(creds_file, _CRED_TOPICS, ",".join(topics))

    token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()

    url = _mcp_url(infra)

    if client == "none":
        print(f"\n✓ RTCE enabled on: {', '.join(topics)} (no coding agent registered)")
        if args.dry_run:
            return
        ok, status = _test_mcp_connection(url, token)
        print(f"  MCP endpoint check: {'✓' if ok else '⚠'} ({status or 'connection error'})")
        return

    if client == "gemini":
        scope_note = _register_gemini(server_name, url, token, dry_run=args.dry_run)
        restart_note = "Restart Gemini to activate."
    elif client == "codex":
        scope_note = _register_with_codex_rtce(server_name, url, token, dry_run=args.dry_run)
        restart_note = "Restart Codex CLI to activate."
    else:
        scope_note = _register_with_claude_code(server_name, url, token, dry_run=args.dry_run)
        restart_note = "Restart Claude Code to activate."

    print(f"\n✓ RTCE MCP server registered as '{server_name}' ({scope_note})")
    print(f"  Topics:   {', '.join(topics)}")
    print(f"  Endpoint: {url}")

    if args.dry_run:
        print("\n[dry-run] Skipping MCP connection test (no config was written).")
        return

    print("\nTesting MCP connection...", end=" ", flush=True)
    ok, status = _test_mcp_connection(url, token)
    if ok:
        print(f"✓ ({status}) MCP connection test passed — agent can query your topics")
    elif status in (401, 403):
        print(
            f"✗ ({status}) Global API key is invalid or lacks RTCE roles "
            f"(DeveloperRead + SchemaRegistryRead).\n"
            f"  Update CONFLUENT_RTCE_API_KEY / CONFLUENT_RTCE_API_SECRET in credentials.env\n"
            f"  and re-run: uv run setup-rtce"
        )
    elif status == 404:
        print(
            "⚠ (404) RTCE MCP endpoint not yet provisioned for this cluster.\n"
            "  Try connecting in Claude Code (/mcp) in a few minutes."
        )
    else:
        code_str = str(status) if status else "connection error"
        print(f"⚠ ({code_str}) Unexpected response — verify the endpoint manually.")

    print(f"\n  {restart_note}")


if __name__ == "__main__":
    main()
