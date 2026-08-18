# Design: Self-Service Workshop Path

**Date:** 2026-08-18
**Status:** Approved (pending spec review)

## Problem / Goal

The repo currently ships as a **one-command demo**: `terraform apply` provisions all
Confluent Cloud infrastructure *and* deploys every Flink statement (tables, model,
functions, tools, agent, windowing, detection). A presenter then runs the producer and
dashboard and walks the audience through the running pipeline.

We want to add a **second path**: a self-service **workshop** where participants deploy
the infrastructure themselves, then **build the AI pipeline by hand** by running the Flink
SQL statements one at a time from the Confluent Cloud Flink UI. The learning value is in
typing/reading the SQL — the model, the function/tool/agent definitions, the windowing, and
the detection query — so those must **not** be auto-deployed in workshop mode.

The demo path must remain unchanged and remain the default.

## Non-Goals

- No change to the fraud-detection logic, prompts, UDFs, producer, or dashboard.
- Participants do **not** create the Bedrock connection or upload the tools JAR by hand
  (a binary artifact upload and AWS credentials in the SQL UI are brittle) — those stay in
  Terraform.
- Not building a fully separate Terraform root/workspace; a single root with a mode flag.

## Key Decisions (from brainstorming)

1. **Manual boundary:** Terraform provisions infra **plus the 4 source/sink tables**.
   Participants hand-run the **10 pipeline statements**: `CREATE MODEL`, 3× `CREATE
   FUNCTION`, 3× `CREATE TOOL`, `CREATE AGENT`, the windowing `CREATE TABLE
   activity_profiles AS …`, and the `INSERT INTO fraud_alerts …` detection.
   - Tables staying in Terraform is deliberate: they carry the Avro value schema + the
     `event_time` watermark, so the producer can run immediately and fill topics while
     participants build the AI pipeline.
2. **Mode toggle:** one boolean variable `deploy_flink_pipeline` (default `true` = demo,
   unchanged) plus a committed `workshop.tfvars.example` preset that sets it `false`.
3. **SQL source:** participants copy the statements **inline from `WORKSHOP.md`**. `flink.tf`
   remains the demo source of truth; `WORKSHOP.md` carries its own inline copies. A
   "keep in sync" note is added to `CLAUDE.md`.
4. **Docs layout:** `README.md` becomes a **landing page** describing the use-case and the
   two paths, linking to `DEMO.md` (demo path) and `WORKSHOP.md` (workshop path).
5. **Doc rename to avoid a case collision:** the existing `demo.md` (the presenter tour) is
   renamed to `WALKTHROUGH.md` via `git mv`, freeing the name `DEMO.md` for the demo-path
   deploy steps. (`DEMO.md` and `demo.md` would collide on case-insensitive filesystems
   like macOS APFS.)

## Architecture / Changes

### 1. Terraform — gate the AI pipeline behind a flag

**`terraform/variables.tf`** — add:

```hcl
variable "deploy_flink_pipeline" {
  description = "Deploy the Flink AI pipeline statements (model, functions, tools, agent, windowing, detection). true = one-command demo. Set false for the self-service workshop, where participants run these statements by hand in the Flink UI."
  type        = bool
  default     = true
}
```

**`terraform/flink.tf`** — add `count = var.deploy_flink_pipeline ? 1 : 0` to the **10
pipeline modules** and nothing else:

- `module.model`
- `module.fn_flag`, `module.fn_freeze`, `module.fn_notify`
- `module.tool_flag`, `module.tool_freeze`, `module.tool_notify`
- `module.agent`
- `module.profiles`
- `module.detect`

**Not gated (always deployed in both modes):**

- Everything in `main.tf` (environment, cluster, SR, compute pool, service account, keys,
  ACLs).
- `confluent_flink_connection.bedrock` and `confluent_flink_artifact.tools` (`flink.tf`).
- The 4 table modules: `module.tbl_transactions`, `module.tbl_user_logins`,
  `module.tbl_account_changes`, `module.tbl_fraud_alerts`.

**Correctness note — `count` on modules is safe here:** no un-gated resource references a
gated module. The gated modules' `depends_on` blocks reference each other (e.g. `detect`
depends on `agent`, `profiles`) and un-gated tables — all present in demo mode, all absent
together in workshop mode. `outputs.tf` and `connect.tf` reference only un-gated resources
(`confluent_flink_artifact.tools.id`, cluster/SR/keys). Adding `count` turns a module into
a list in address space; because nothing interpolates `module.<gated>.*` outside the gated
set, no `[0]` indexing fixups are needed elsewhere.

### 2. New outputs for the manual SQL

**`terraform/outputs.tf`** — add two outputs so the workshop SQL is fill-in-the-blank
(the Flink UI catalog/database dropdowns show *display names*, not ids):

```hcl
output "flink_catalog" {
  value       = confluent_environment.main.display_name
  description = "Flink catalog to select in the workspace (environment display name)"
}

output "flink_database" {
  value       = confluent_kafka_cluster.standard.display_name
  description = "Flink database to select in the workspace (cluster display name)"
}
```

`tools_artifact_id` already exists and is reused to fill the 3 `CREATE FUNCTION` statements.

### 3. `workshop.tfvars.example`

New file **`terraform/workshop.tfvars.example`** — mirrors `terraform.tfvars.example` (the
4 credential placeholders) plus the mode flag:

```hcl
confluent_cloud_api_key    = "..."
confluent_cloud_api_secret = "..."
aws_access_key_id          = "..."
aws_secret_access_key      = "..."

# Workshop mode: provision infra + tables + Bedrock connection + tools JAR only.
# Participants run the AI pipeline statements by hand from the Flink UI (see WORKSHOP.md).
deploy_flink_pipeline = false
```

`.gitignore` already commits `*.tfvars.example` and ignores `*.tfvars` — no change needed.
Participants: `cp workshop.tfvars.example workshop.tfvars`, fill creds, then
`terraform apply -var-file=workshop.tfvars`.

### 4. `WORKSHOP.md` (new, repo root)

Structure, targeting a **1-hour hands-on budget**:

1. **Before you arrive (prerequisites — do these in advance).** Bedrock Claude model access
   in `us-east-1` (out of your control if approval lags → must be pre-done); Confluent Cloud
   account + Cloud API key; AWS IAM user + access key with `bedrock:InvokeModel`; Terraform,
   Git, Python 3.11+ installed. This section is explicitly *outside* the timed hour.
2. **Deploy the infrastructure (~8–12 min).** `cp workshop.tfvars.example workshop.tfvars`,
   fill creds, `terraform init && terraform apply -var-file=workshop.tfvars`. Note the
   outputs: `tools_artifact_id`, `flink_catalog`, `flink_database`.
3. **Open the Flink workspace and set context.** Select catalog = `flink_catalog`,
   database = `flink_database`.
4. **Build the pipeline — run these 10 statements in order (~15–25 min).** Each statement
   inline in its own code fence with a one-line explanation:
   1. `CREATE MODEL fraud_model` (references connection `bedrock-fraud-connection`).
   2–4. `CREATE FUNCTION flag_transaction | freeze_account | notify_user` — each
      `USING JAR 'confluent-artifact://<TOOLS_ARTIFACT_ID>'`; instruct participants to paste
      the `tools_artifact_id` output into `<TOOLS_ARTIFACT_ID>`.
   5–7. `CREATE TOOL flag_transaction_tool | freeze_account_tool | notify_user_tool`.
   8. `CREATE AGENT fraud_detection_agent` (full hardened prompt, `USING MODEL`,
      `USING TOOLS`, `max_iterations`/`handle_exception`/`max_consecutive_failures`).
   9. Windowing: **first** run `SET 'sql.tables.scan.idle-timeout' = '5 s';`, **then**
      `CREATE TABLE activity_profiles AS …`. Call out *why* the `SET` matters (in demo mode
      this is `module.profiles`'s `extra_properties`; in the UI it must be a session `SET`
      before the CTAS — without it, alerts arrive as an early burst then dry up).
   10. Detection: `INSERT INTO fraud_alerts … AI_RUN_AGENT(…)`.
   - Note: DDL statements (1–8) reach `COMPLETED` in seconds; 9 and 10 go `RUNNING`.
5. **Run the apps (~8–12 min).** `pip install -r requirements.txt`, run
   `producer/generate_events.py` and `streamlit run dashboard/app.py`. The `.env` written by
   Terraform works in both modes. Allow ~1–2 min for the first window-firing batch of alerts.
6. **Cleanup.** `terraform destroy -var-file=workshop.tfvars`.

All SQL bodies are copied verbatim from `flink.tf` (only substitution: `<TOOLS_ARTIFACT_ID>`
placeholder in the 3 functions, and the `SET` replacing the windowing `extra_properties`).

### 5. `README.md` → landing page

Restructure into:

- **Use-case + architecture** — keep the current intro, the 4-stage "What happens" table,
  and the Stream Lineage image.
- **Prerequisites** — shared by both paths (keep the current Prerequisites section here).
- **Choose your path** — a new section with two clearly contrasted options:
  - **🎬 Demo path** — one command, everything auto-deployed, ~15 min → link to `DEMO.md`.
  - **🛠️ Self-service workshop** — deploy infra + tables, build the AI pipeline by hand in
    the Flink UI, ~1 hour → link to `WORKSHOP.md`.
- **Shared reference** — keep Directory Structure, "How the agent's tools work",
  Troubleshooting, Cleanup, and the early-access footer in README.
- Remove the inline one-command Quick Start (it moves to `DEMO.md`). Fix the `demo.md` link
  (now the demo path lives in `DEMO.md`, which links onward to `WALKTHROUGH.md`).

### 6. `DEMO.md` (new)

The demo path: the current README "Quick Start" (steps 1–6: clone, 4 creds, one-command
`terraform apply`, install deps, run producer, run dashboard) plus the "Demo walkthrough"
pointer. Ends by linking to **`WALKTHROUGH.md`** for the guided tour.

### 7. `demo.md` → `WALKTHROUGH.md` (rename)

`git mv demo.md WALKTHROUGH.md`. Content unchanged (its `images/demo/…` references still
resolve). Update the H1/intro wording only if it references the old filename (it does not).
Update all inbound links: `README.md` and `CLAUDE.md`.

### 8. `CLAUDE.md` updates

- Document **workshop mode**: the `deploy_flink_pipeline` flag, `workshop.tfvars.example`,
  what is / isn't deployed, and that the 10 pipeline statements are run by hand.
- Update the **Repository map** and any `demo.md` references to the new doc layout
  (`README.md` landing page, `DEMO.md`, `WORKSHOP.md`, `WALKTHROUGH.md`).
- Add a **"keep `WORKSHOP.md` SQL in sync with `flink.tf`"** note under Key conventions
  (the two are parallel copies by design).

## Data Flow

Unchanged from the demo. In workshop mode the same statements exist — they are created by
the participant in the UI rather than by Terraform. Runtime behavior (windowing, agent,
alerts, idle-timeout) is identical once all 10 statements are running.

## Testing / Verification

- **Static:** `cd terraform && terraform fmt -recursive && terraform validate` (both modes
  parse). `terraform plan -var-file=workshop.tfvars` (with dummy creds via `plan` only) should
  show the 10 pipeline modules absent and infra + 4 tables + connection + artifact present;
  default `plan` shows the full set. (Full `apply` needs real credentials — cannot run here.)
- **Docs:** verify no remaining `demo.md` references; verify README → `DEMO.md`/`WORKSHOP.md`
  links and `DEMO.md` → `WALKTHROUGH.md` link resolve; verify every SQL block in `WORKSHOP.md`
  matches the corresponding statement body in `flink.tf` (modulo the `<TOOLS_ARTIFACT_ID>`
  placeholder and the `SET`).
- **Manual end-to-end (requires credentials, done by a maintainer):** workshop apply →
  run the 10 statements in order → producer + dashboard → confirm alerts flow, per the
  existing CLAUDE.md "Testing / verifying end-to-end" procedure.

## Risks / Open Points

- **SQL drift** between `WORKSHOP.md` and `flink.tf` — mitigated by the CLAUDE.md sync note;
  accepted because the user wants copy-from-README ergonomics.
- **The `SET` idle-timeout** is the one non-obvious manual step; called out prominently in
  `WORKSHOP.md` with the reason, since regressing it silently degrades the demo.
- **Hour budget** holds only if prerequisites (esp. Bedrock model access) are pre-done;
  `WORKSHOP.md` gates them as step 0, outside the timed portion.
