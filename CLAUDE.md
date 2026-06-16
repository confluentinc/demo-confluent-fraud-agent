# CLAUDE.md

Guidance for working in this repository.

## What this is

Real-time fraud detection running entirely on **Confluent Cloud**. Streaming user
activity is windowed by **Confluent Cloud for Apache Flink** and analyzed by a
**Streaming Agent** (Confluent Intelligence) backed by **AWS Bedrock (Claude)**, which
emits fraud alerts. A local Python **producer** generates synthetic events and a local
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
        [Bedrock Claude + 3 UDF tools] → parse JSON verdict → fraud_alerts (topic)
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
| `terraform/flink.tf` | Bedrock connection, tools artifact upload, and every Flink statement: source tables, `CREATE MODEL`, `CREATE FUNCTION`/`CREATE TOOL` ×3, `CREATE AGENT`, the `create-activity-profiles` windowing statement (`module.profiles`), and the `detect-fraud` agent `INSERT` (`module.detect`). |
| `terraform/connect.tf` | Writes the repo-root `.env` consumed by the local apps. |
| `terraform/variables.tf` | The **4** required inputs (Confluent key/secret, AWS Bedrock IAM key/secret). |
| `terraform/modules/flink-statement/` | Thin reusable wrapper around `confluent_flink_statement`. |
| `tools-udf/` | Java UDF tools (`flag_transaction`, `freeze_account`, `notify_user`) + the committed `target/fraud-tools.jar`. |
| `producer/generate_events.py` | Synthetic event generator (Avro + SASL_SSL). |
| `dashboard/app.py` | Streamlit dashboard (Avro + SASL_SSL); Recent Fraud Alerts table shows severity/user/score/time/reasoning/**actions**. |
| `agent/models.py` | Pydantic data-model reference only — not imported at runtime. |
| `demo.md` | ~15-min presenter walkthrough (Stream Lineage → Flink job → dashboard); screenshots in `images/demo/`. Linked from the README. |

## Commands

```bash
# Deploy everything (also writes .env)
cd terraform && terraform init && terraform apply

# Run apps (after deploy)
pip install -r requirements.txt
python producer/generate_events.py
streamlit run dashboard/app.py

# Tear down
cd terraform && terraform destroy

# Validate Terraform without credentials
cd terraform && terraform fmt -recursive && terraform validate

# Rebuild the tools JAR (maintainers only; needs Docker — colima/Docker Desktop)
cd tools-udf && ./build.sh    # then commit target/fraud-tools.jar
```

## Key conventions & constraints

- **4 inputs only.** Region (`us-east-1`), Claude model, resource names, and sizing are
  fixed in `terraform/main.tf` `locals`. Don't add user-facing variables without reason.
- **Bedrock model id lives in the connection endpoint URL** (`local.bedrock_model_id` →
  `local.bedrock_endpoint`), NOT in `CREATE MODEL`. Change that one line to switch models.
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
- **Streamlit's first-run email prompt blocks headless/background runs.** Write
  `~/.streamlit/credentials.toml` with `[general]\nemail = ""` and run with
  `--server.headless true`, then open `http://localhost:8501` manually.
- **You can't `terraform apply` without the user's Confluent + AWS credentials.** Validate
  with `terraform validate`.
- **Bedrock model access:** the IAM user must have model access granted in the Bedrock
  console for the configured Claude model in `us-east-1`.
- **Producer/consumer Python stdout is buffered** when backgrounded — absence of printed
  cycles ≠ failure. Verify by consuming topics, not by reading producer stdout.
- **Docker runtime here is colima**, not Docker Desktop (`colima start` before `build.sh`).
- **Pushing:** remote is `confluentinc/demo-confluent-fraud-agent`; work on a branch. Always
  push with **`git push-external`** (never bare `git push` — it's blocked); it runs a
  proprietary-code check + Airlock push.
- Reference material: the official `confluentinc/quickstart-streaming-agents` repo (esp.
  `lab4-pubsec-fraud-agents`, `lab3`) is the source for verified CC Streaming Agents syntax.

## Testing / verifying end-to-end

All checks below assume `terraform apply` has run (so `.env` exists) and the venv is set up
(`python3 -m venv venv && ./venv/bin/pip install -r requirements.txt`). Use `./venv/bin/python`
and load `.env` for connection config.

### 1. Static checks (no credentials needed)
```bash
cd terraform && terraform fmt -recursive && terraform validate
python3 -m py_compile producer/generate_events.py dashboard/app.py
unzip -l tools-udf/target/fraud-tools.jar   # 3 classes: FlagTransaction/FreezeAccount/NotifyUser
```

### 2. Deploy and confirm all statements are healthy
```bash
cd terraform && terraform init && terraform apply -auto-approve   # 31 resources; writes ../.env
```
Confirm Flink statements via the REST API (the CLI session often expires). Pull the Flink key
+ endpoint from state, then list statement phases — DDL (`create-table-*`, `create-model-*`,
`create-function-*`, `create-tool-*`, `create-agent-*`) should be `COMPLETED`, and the two
continuous statements `create-activity-profiles-*` and `detect-fraud-*` should be `RUNNING`:
```bash
cd terraform && terraform show -json > /tmp/tf.json
python3 - <<'PY'
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
  | python3 -c "import sys,json;[print(s['name'].ljust(40),s['status']['phase'],'|',(s['status'].get('detail') or '')[:80]) for s in json.load(sys.stdin)['data']]"
```

### 3. Run the producer, then verify data + alerts flow
```bash
./venv/bin/python producer/generate_events.py   # leave running (background ok)
```
Consume the four topics to confirm input is flowing AND alerts are produced. Save as
`/tmp/verify.py` and run with `./venv/bin/python /tmp/verify.py`:
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
printf '[general]\nemail = ""\n' > ~/.streamlit/credentials.toml   # one-time, skips prompt
./venv/bin/streamlit run dashboard/app.py --server.headless true --server.port 8501
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8501/_stcore/health   # 200
```
Open `http://localhost:8501`. The dashboard reads from `latest`, so it shows alerts produced
after it starts — keep the producer running and allow ~1 min for a window-firing batch.

### 6. Teardown
```bash
cd terraform && terraform destroy -auto-approve
```
