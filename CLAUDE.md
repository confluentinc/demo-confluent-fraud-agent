# CLAUDE.md

Guidance for working in this repository.

## What this is

Real-time fraud detection running entirely on **Confluent Cloud**. Streaming user
activity is windowed by **Confluent Cloud for Apache Flink** and analyzed by a
**Streaming Agent** (Confluent Intelligence) backed by **Claude** — CSP Hosted Flink Model
Inference by default (`'provider' = 'confluent'`, no AWS keys), or the user's own AWS Bedrock
with `uv run deploy --bedrock` — which emits fraud alerts. A local Python **producer** generates synthetic events and a local
**Streamlit dashboard** visualizes them.

Everything except the producer and dashboard runs in Confluent Cloud. There is **no local
Kafka/Flink** — the previous Docker/PyFlink implementation was removed.

## Architecture / data flow

```
producer (local, Avro+SASL) → transactions · user_logins · account_changes (topics)
  → [statement 1: create-activity-profiles] UNION ALL the 3 streams
        → 3s event-time SESSION window per user
        → activity_profiles table/topic  (one text profile per user per window; queryable)
  → [statement 2: detect-fraud] AI_RUN_AGENT(fraud_detection_agent, profile_text, user_id)
        [Claude (CSP Hosted | Bedrock) + 3 UDF tools] → parse JSON verdict → fraud_alerts (topic)
  → dashboard (local, Avro+SASL)
```

The windowing and the agent are **two separate continuous statements** (split so the
windowed profiles are materialized as the queryable `activity_profiles` table and show up as
a node in Stream Lineage). All Flink/agent/model/tool SQL is defined inline in
`terraform/flink.tf` and deployed as `confluent_flink_statement` resources via the
`terraform/modules/flink-statement` wrapper.

## Repository map

| Path | Purpose |
|------|---------|
| `terraform/main.tf` | Env, cluster, Schema Registry, compute pool, service account, API keys, ACLs, and all fixed `locals` (region, model id, names). |
| `terraform/flink.tf` | `llm_provider` switch (`local.fraud_model_with`) + the Bedrock connection (only for `bedrock`), tools artifact upload, and every Flink statement: source tables, `CREATE MODEL`, `CREATE FUNCTION`/`CREATE TOOL` ×3, `CREATE AGENT`, the `create-activity-profiles` windowing statement (`module.profiles`), and the `detect-fraud` agent `INSERT` (`module.detect`). |
| `terraform/connect.tf` | Writes the repo-root `.env` consumed by the local apps. |
| `terraform/variables.tf` | Confluent key/secret (required), `llm_provider` (`confluent` default \| `bedrock`) + `csp_hosted_model`, `aws_bedrock_access_key`/`aws_bedrock_secret_key`/`aws_session_token` (only used for `bedrock`, default `""`), and the `deploy_flink_pipeline` mode flag (default `true`). |
| `terraform/outputs.tf` | Includes the shared-name outputs (`app_manager_*_api_key`, `confluent_*_endpoint`, …) that `scripts/mcp_setup.py` reads — same names as `terraform/core` in the sibling streaming-agent repos. |
| `scripts/` | Flat package behind the `uv run` entry points in `pyproject.toml`: `deploy`, `destroy`, `dashboard`, `setup-mcp`, `setup-rtce`, `api-keys`, `test-bedrock` (+ shared helpers `credentials`, `login_checks`, `terraform`, `terraform_runner`, `tfvars`, `ui`). Ported from the Streamhouse / quickstart-streaming-agents repos; keep them shaped alike. |
| `credentials.env` | Gitignored; written by `uv run deploy` (template: `credentials.env.example`). Deploy generates `terraform/terraform.tfvars` from it on every run. |
| `terraform/rtce.tf` | Read-only RTCE service account for `uv run setup-rtce` (not used by the pipeline; kept for later). |
| `terraform/modules/flink-statement/` | Thin reusable wrapper around `confluent_flink_statement`. |
| `tools-udf/` | Java UDF tools (`flag_transaction`, `freeze_account`, `notify_user`) + the committed `target/fraud-tools.jar`. |
| `producer/generate_events.py` | Synthetic event generator (Avro + SASL_SSL). |
| `dashboard/app.py` | Streamlit dashboard (Avro + SASL_SSL); Recent Fraud Alerts table shows severity/user/score/time/reasoning/**actions**. |
| `agent/models.py` | Pydantic data-model reference only — not imported at runtime. |
| `README.md` | Landing page: use-case + architecture + shared prerequisites, then "Choose your path" linking to `DEMO.md` / `WORKSHOP.md`. |
| `DEMO.md` | Demo path: one-command `uv run deploy` steps; links to `WALKTHROUGH.md`. |
| `WORKSHOP.md` | Self-service workshop path: deploy infra + tables + functions via `uv run deploy --workshop` (`deploy_flink_pipeline=false`), then run the hands-on statements (model, tools, agent, windowing, detect) by hand in the Flink UI. Inline SQL must match `flink.tf`. |
| `WALKTHROUGH.md` | ~15-min presenter walkthrough (Stream Lineage → Flink job → dashboard); screenshots in `images/demo/`. Linked from `DEMO.md`. |

## Commands

```bash
# Deploy everything (prompts once, saves credentials.env, writes tfvars + .env)
uv run deploy                 # --workshop: workshop mode; --bedrock: own AWS Bedrock keys instead of CSP Hosted;
                              # --testing/--automated: non-interactive from credentials.env

# Run apps (after deploy)
uv run producer
uv run dashboard              # headless streamlit + opens the browser; --no-browser for bots

# Give a coding agent the Confluent MCP server (Flink SQL, topics) for this deployment
uv run setup-mcp --agent claude   # then restart Claude Code

# Tear down
uv run destroy                # --testing: non-interactive

# Tests + lint
uv run pytest && uv run ruff check scripts tests

# Validate Terraform without credentials
cd terraform && terraform fmt -recursive && terraform validate

# Rebuild the tools JAR (maintainers only; needs Docker — colima/Docker Desktop)
cd tools-udf && ./build.sh    # then commit target/fraud-tools.jar
```

## Key conventions & constraints

- **Minimal inputs.** Only the Confluent key/secret are required (plus the Bedrock key/secret
  with `--bedrock`). Region (`us-east-1`), resource names, and sizing are fixed in
  `terraform/main.tf` `locals`. Don't add user-facing variables without reason. (The exceptions
  are the `deploy_flink_pipeline` mode flag below and the `llm_provider` switch.)
- **Two LLM providers via `llm_provider`.** Default `confluent` = CSP Hosted Flink Model
  Inference: `fraud_model` is `'provider' = 'confluent'`, `'confluent.model' =
  var.csp_hosted_model` (`anthropic.claude-sonnet-4-6`), no connection, no AWS keys, and deploy
  skips the AWS prompts and the Bedrock check. `bedrock` (`uv run deploy --bedrock`, saved as
  `TF_VAR_llm_provider` in `credentials.env`) creates `confluent_flink_connection.bedrock[0]`
  (count-gated; a `moved` block keeps pre-switch state) and renders the exact Bedrock
  `CREATE MODEL` already applied, so existing Bedrock deployments plan as a no-op. Deploy resolves
  the provider as: `--bedrock` / exported `TF_VAR_llm_provider`, else **what Terraform state
  shows is deployed** (the Bedrock connection exists ⇒ `bedrock`), else `credentials.env`, else
  `confluent` (`scripts/tfvars.py` `llm_provider_for`). State wins over the saved value so a live
  Bedrock deployment is never silently swapped. **Switching provider on an applied deployment
  re-runs `CREATE MODEL` against an existing name — `uv run destroy` first.**
- **Two deployment paths via `deploy_flink_pipeline`.** Default `true` = the one-command
  demo (deploys everything). `false` (`uv run deploy --workshop`, persisted as
  `TF_VAR_deploy_flink_pipeline=false` in `credentials.env`) = workshop
  mode: Terraform deploys infra + (with `--bedrock`) the Bedrock connection + tools JAR + the 4 tables **+ the 3
  `CREATE FUNCTION` statements** (`fn_*`, ungated — they reference the artifact id, so we don't
  make participants handle it). The 7 hands-on modules (`model`, `tool_*`, `agent`, `profiles`,
  `detect`) are gated off with `count` so participants run them by hand from the Flink UI per
  `WORKSHOP.md`.
- **Keep `WORKSHOP.md` SQL in sync with `flink.tf`.** `WORKSHOP.md` embeds copies of the
  hands-on statements for participants to paste (model, the 3 tools, agent, windowing, detect —
  **not** the functions, which Terraform creates). When you change one of those in `flink.tf`,
  update the matching block in `WORKSHOP.md`. Note: only `flag_transaction_tool` is shown in
  full; `freeze_account_tool` / `notify_user_tool` are a "your turn" challenge shown with the
  `USING FUNCTION` name replaced by a `<function-name>` placeholder (a `<details>` hint points
  participants at `SHOW USER FUNCTIONS`). The workshop has no `CREATE MODEL` step: the agent's `USING MODEL`
  is the built-in `system.anthropic.claude-sonnet-4-6`. The detect `INSERT`'s `AI_RUN_AGENT(...)` uses
  an `<agent-name>` placeholder challenge (hint: `SHOW AGENTS`). Section 6's "Give the agent its
  model" challenge offers two options: **Option A** — the guided Streaming Agents UI (steps + the
  fraud-policy prompt shown as a plain-text block for the Instructions field); **Option B** — the
  `CREATE AGENT` SQL inside a `<details>`. **The prompt therefore appears twice in `WORKSHOP.md`**
  and both copies must track `flink.tf`'s prompt: Option A is plain text (single apostrophe,
  `user's`), Option B is the SQL string (doubled apostrophe, `user''s`) — that `'`/`''` is the
  only allowed difference between them. The intended differences from `flink.tf`: (a) the windowing
  statement is preceded by `SET 'sql.tables.scan.idle-timeout' = '5 s';` (in `flink.tf` that
  lives in `module.profiles`'s `extra_properties`); (b) the windowing statement is also preceded
  by `SET 'sql.tables.scan.startup.mode' = 'latest-offset';` — **workshop only** (demo
  intentionally reprocesses from earliest); and (c) both continuous statements carry a
  `SET 'client.statement-name' = '…';`. Latest-offset keeps each participant's agent calls
  proportional to live traffic instead of replaying topic history through the model.
- **Where the model id lives depends on the provider.** CSP Hosted: `var.csp_hosted_model`
  (`'confluent.model'` in `CREATE MODEL`; case-sensitive). Bedrock: the connection endpoint URL
  (`local.bedrock_model_id` → `local.bedrock_endpoint`), NOT in `CREATE MODEL`. WORKSHOP.md
  is CSP Hosted only (no Bedrock/AWS prerequisites, prompts, or `CREATE MODEL` variant); keep its
  model block in sync with `flink.tf`.
- **Topic = Flink table name**, snake_case: `transactions`, `user_logins`,
  `account_changes`, `fraud_alerts`. Keep producer/dashboard/SQL in sync if renaming.
- **Avro, Flink owns the schema.** Source tables are created by Flink (`CREATE TABLE`,
  default avro-registry). The producer serializes with `use.latest.version=True` /
  `auto.register.schemas=False` so it encodes against Flink's registered schema. Producer
  Avro field names/order must match the table columns; physical columns only (the computed
  `event_time` is not in the value schema).
- **Windowing is materialized as `activity_profiles`.** `module.profiles` unions the 3 streams
  and applies an event-time `SESSION` window (3s gap, partitioned by `user_id`) → writes the
  `activity_profiles` table/topic (queryable: `SELECT * FROM activity_profiles`). `module.detect`
  reads it and runs the agent. SESSION (not `TUMBLE`) so a fraud burst is never split across
  windows. `event_time` is a computed `TO_TIMESTAMP_LTZ(`timestamp`, 3)` column with a 5s
  watermark, and the idle-timeout lives on `module.profiles` (where the windowing is).
- **Agent output is parsed from free text.** `AI_RUN_AGENT` returns a `response` STRING; the
  detect query extracts the JSON object via `REGEXP_EXTRACT` then `JSON_VALUE`/`JSON_QUERY`.
  `fraud_alerts.actions_taken` / `flagged_transaction_ids` are stored as JSON-array strings;
  the dashboard coerces them back to lists.
- **The 3 agent tools are mocks** (return a confirmation string, no side effect) but are
  genuinely invoked by the agent. Editing them means editing Java + rebuilding the JAR.
- **Flink SQL string literals don't process backslash escapes** — regex like `'\{[\s\S]*\}'`
  is passed literally to the Java regex engine (intended).

## Gotchas

- **The agent statement stalls on malformed model output if not hardened.** `CREATE AGENT`
  fails the whole statement after `max_consecutive_failures` (default **3**) consecutive bad
  agent runs — e.g. when Claude calls a tool then reverses it / "no tools needed" / emits
  non-JSON. Mitigated on `module.agent` (`flink.tf`) by: (a) a hardened prompt (decide score
  first, make all tool calls up front, never reverse a call, final message = the single JSON
  object only), (b) `handle_exception = 'continue'` (skip a bad row instead of counting it as
  a failure), and (c) `max_consecutive_failures = '5'`. **Symptom if regressed:** `detect-fraud`
  goes FAILED after a few minutes and the dashboard empties out, even though model access is
  fine. (`handle_exception` wasn't in the docs at time of writing but is accepted by the API.)
- **Watermark idle-timeout is required for steady alerts.** Confluent Cloud's default
  "progressive idleness" grows the idle-partition timeout with statement age (10s → up to
  5 min). With 6-partition topics and a producer that only touches some users per cycle, the
  session-window watermark stalls as the statement ages → alerts come as an early burst then
  dry up. Fixed by pinning `sql.tables.scan.idle-timeout = '5 s'` on the **windowing** statement
  (`module.profiles` `extra_properties` in `flink.tf` — that's where the SESSION window is).
  **Symptom if regressed:** `activity_profiles` / `fraud_alerts` grow fast at first, then stop
  while input topics keep filling. Alerts still arrive in small batches (window-firing is
  inherently batchy over keyed bursty data) — that's expected.
- **Changing a Flink statement's SQL or `properties` needs replace, not in-place update.** The
  provider errors ("stopped attribute must be updated…"). Use
  `terraform apply -replace="module.<name>.confluent_flink_statement.this"` (e.g. `module.detect`
  or `module.profiles`). A replaced INSERT/CTAS reprocesses from earliest (duplicate rows; fine
  for the demo).
- **Streamlit's first-run email prompt blocks non-headless runs.** `uv run dashboard` always
  passes `--server.headless true` (no prompt) and opens the browser itself once
  `/_stcore/health` is up; `--no-browser` for background runs.
- **Never run full `terraform apply` for a targeted change** — use `-target`. `uv run deploy` is the
  full-stack path.
- **You can't deploy without the user's Confluent credentials** (plus AWS credentials with
  `--bedrock`) in `credentials.env`. Validate with `terraform validate`.
- **Bedrock model access (`--bedrock` only):** the IAM user must have model access granted in the
  Bedrock console for the configured Claude model in `us-east-1`.
- **Producer/consumer Python stdout is buffered** when backgrounded — absence of printed
  cycles ≠ failure. Verify by consuming topics, not by reading producer stdout.
- **Docker runtime here is colima**, not Docker Desktop (`colima start` before `build.sh`).
- **Pushing:** remote is `confluentinc/demo-confluent-fraud-agent`; work on a branch. Always
  push with **`git push-external`** (never bare `git push` — it's blocked); it runs a
  proprietary-code check + Airlock push.
- Reference material: the official `confluentinc/quickstart-streaming-agents` repo (esp.
  `lab4-pubsec-fraud-agents`, `lab3`) is the source for verified CC Streaming Agents syntax.

## Testing / verifying end-to-end

All checks below assume `uv run deploy` has run (so `.env` exists). Use `uv run python`
and load `.env` for connection config. With `uv run setup-mcp --agent claude` (and a Claude Code
restart) the `confluent-cloud-mcp-server` MCP tools can list statements, run Flink SQL, and
consume topics directly — prefer those over the REST/consumer snippets below.

### 1. Static checks (no credentials needed)
```bash
cd terraform && terraform fmt -recursive && terraform validate
uv run pytest && uv run ruff check scripts tests
uv run python -m py_compile producer/generate_events.py dashboard/app.py
unzip -l tools-udf/target/fraud-tools.jar   # 3 classes: FlagTransaction/FreezeAccount/NotifyUser
```

### 2. Deploy and confirm all statements are healthy
```bash
uv run deploy --automated   # non-interactive from credentials.env; writes .env, registers the MCP server
```
Confirm Flink statements via the REST API (the CLI session often expires). Pull the Flink key
+ endpoint from state, then list statement phases — DDL (`create-table-*`, `create-model-*`,
`create-function-*`, `create-tool-*`, `create-agent-*`) should be `COMPLETED`, and the two
continuous statements `create-activity-profiles-*` and `detect-fraud-*` should be `RUNNING`:
```bash
cd terraform && terraform show -json > /tmp/tf.json
uv run python - <<'PY'
import json
d=json.load(open("/tmp/tf.json")); res={}
def walk(m):
    for r in m.get("resources",[]): res.setdefault(r["type"]+"."+r["name"], r["values"])
    for c in m.get("child_modules",[]): walk(c)
walk(d["values"]["root_module"])
fk=res["confluent_api_key.flink"]; reg=next(v for k,v in res.items() if "flink_region" in k)
print("FLINK_KEY="+fk["id"]); print("FLINK_SECRET="+fk["secret"]); print("FLINK_REST="+reg["rest_endpoint"])
print("ENV="+res["confluent_environment.main"]["id"]); print("ORG="+res["confluent_organization.main"]["id"])
PY
# then (substitute the printed values):
curl -s -u "$FLINK_KEY:$FLINK_SECRET" \
  "$FLINK_REST/sql/v1/organizations/$ORG/environments/$ENV/statements?page_size=50" \
  | uv run python -c "import sys,json;[print(s['name'].ljust(40),s['status']['phase'],'|',(s['status'].get('detail') or '')[:80]) for s in json.load(sys.stdin)['data']]"
```

### 3. Run the producer, then verify data + alerts flow
```bash
uv run producer   # leave running (background ok)
```
Consume the four topics to confirm input is flowing AND alerts are produced. Save as
`/tmp/verify.py` and run with `uv run python /tmp/verify.py`:
```python
import os, json, collections, time
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
load_dotenv(".env")
c=Consumer({"bootstrap.servers":os.environ["BOOTSTRAP_SERVERS"],"security.protocol":"SASL_SSL",
 "sasl.mechanisms":"PLAIN","sasl.username":os.environ["KAFKA_API_KEY"],
 "sasl.password":os.environ["KAFKA_API_SECRET"],"group.id":f"v{time.time()}","auto.offset.reset":"earliest"})
c.subscribe(["transactions","user_logins","account_changes","fraud_alerts"])
sr=SchemaRegistryClient({"url":os.environ["SCHEMA_REGISTRY_URL"],
 "basic.auth.user.info":f'{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}'})
d=AvroDeserializer(sr); cnt=collections.Counter(); hi=[]; end=time.time()+40
while time.time()<end:
    m=c.poll(1.0)
    if not m or m.error(): continue
    v=d(m.value(),SerializationContext(m.topic(),MessageField.VALUE)); cnt[m.topic()]+=1
    if m.topic()=="fraud_alerts" and v.get("risk_score",0)>=60: hi.append(v)
c.close(); print(dict(cnt))
for a in hi[:5]: print(a["risk_score"], a["user_id"], a["actions_taken"], "-", a["reasoning"][:80])
```
**Pass criteria:** all three input topics increase; `activity_profiles` and `fraud_alerts`
increase; the injected scenarios yield high-risk alerts (geo-impossible ≈90, account-takeover
≈95) with `actions_taken` like `["freeze_account","notify_user"]` and a copied
`flagged_transaction_ids`. (Add `"activity_profiles"` to the `subscribe([...])` list above to
also confirm the windowed profiles flow.)

In the Flink workspace you can also eyeball both stages directly:
```sql
SELECT * FROM activity_profiles;              -- one row per user per session window
SELECT * FROM fraud_alerts WHERE risk_score >= 70;   -- the high-risk verdicts
```

### 4. Confirm alerts keep flowing (watermark / idle-timeout regression check)
A `latest`-offset consumer should keep seeing NEW alerts over a 60–90s window (in small
batches). If counts stall while `transactions` keeps growing, the idle-timeout fix has
regressed — see Gotchas. Topic partition counts / totals:
```bash
# get_watermark_offsets per partition summed = total messages; expect fraud_alerts to grow
```

### 5. Dashboard
```bash
uv run dashboard --no-browser   # headless, so no email prompt
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8501/_stcore/health   # 200
```
Open `http://localhost:8501`. The dashboard reads from `latest`, so it shows alerts produced
after it starts — keep the producer running and allow ~1 min for a window-firing batch.

### 6. Teardown
```bash
uv run destroy --testing
```
