# Self-Service Workshop Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-service workshop deployment path where participants provision infra + tables via Terraform and then run the 10 AI-pipeline Flink statements by hand from the Confluent Cloud UI, while keeping the existing one-command demo as the unchanged default.

**Architecture:** A single Terraform root gains one boolean `deploy_flink_pipeline` (default `true`) that gates the 10 pipeline statement modules with `count`. Infra, the 4 tables, the Bedrock connection, and the tools JAR always deploy. Docs are restructured into a landing-page `README.md` linking to two path docs: `DEMO.md` (one-command) and `WORKSHOP.md` (manual SQL). The existing `demo.md` presenter tour is renamed to `WALKTHROUGH.md` to free the `DEMO.md` name on case-insensitive filesystems.

**Tech Stack:** Terraform (confluentinc/confluent provider), Confluent Cloud for Apache Flink SQL, Markdown docs.

## Global Constraints

- **Demo path stays default and unchanged:** `deploy_flink_pipeline` default MUST be `true`; a plain `terraform apply` deploys everything exactly as today.
- **4 credential inputs only** remain the user-facing variables; the new flag is the sole addition and has a default so it is not required.
- **Terraform must pass** `terraform fmt -recursive` (no diff) and `terraform validate` after every Terraform task.
- **No credentials available in this environment** — never run `terraform apply`. Verification uses `fmt`, `validate`, and code/diff inspection only.
- **WORKSHOP.md SQL must match `flink.tf` verbatim**, except: the 3 `CREATE FUNCTION` statements use the literal placeholder `<TOOLS_ARTIFACT_ID>` in place of `confluent-artifact://${confluent_flink_artifact.tools.id}`, and the windowing statement is preceded by `SET 'sql.tables.scan.idle-timeout' = '5 s';` instead of the module's `extra_properties`.
- **Fixed constant names** referenced by the manual SQL: Bedrock connection display name is `bedrock-fraud-connection`; model `fraud_model`; functions `flag_transaction`/`freeze_account`/`notify_user`; tools `flag_transaction_tool`/`freeze_account_tool`/`notify_user_tool`; agent `fraud_detection_agent`; tables `transactions`/`user_logins`/`account_changes`/`fraud_alerts`/`activity_profiles`.
- **Case-collision rule:** no two committed files may differ only by case. `DEMO.md` and `demo.md` must never coexist — `demo.md` is renamed to `WALKTHROUGH.md` in the same change set that introduces `DEMO.md`.
- **Commit style:** end commit messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Work stays on branch `workshop-path`.

---

### Task 1: Gate the AI pipeline behind `deploy_flink_pipeline`

Add the mode variable, gate the 10 pipeline modules, add the workshop tfvars preset. This is the Terraform behavior change; docs come later.

**Files:**
- Modify: `terraform/variables.tf` (append the new variable)
- Modify: `terraform/flink.tf` (add `count` to 10 modules)
- Create: `terraform/workshop.tfvars.example`

**Interfaces:**
- Produces: variable `deploy_flink_pipeline` (bool, default `true`); when `false`, modules `model`, `fn_flag`, `fn_freeze`, `fn_notify`, `tool_flag`, `tool_freeze`, `tool_notify`, `agent`, `profiles`, `detect` are not created. `terraform apply -var-file=workshop.tfvars` selects workshop mode.

- [ ] **Step 1: Add the variable to `terraform/variables.tf`**

Append at the end of the file:

```hcl

variable "deploy_flink_pipeline" {
  description = "Deploy the Flink AI pipeline statements (model, functions, tools, agent, windowing, detection). true = the one-command demo. Set false for the self-service workshop, where participants run these statements by hand in the Flink UI (see WORKSHOP.md)."
  type        = bool
  default     = true
}
```

- [ ] **Step 2: Gate the 10 pipeline modules in `terraform/flink.tf`**

Add this exact line as the **first line inside the module block** (immediately after `source = "./modules/flink-statement"`) for each of these 10 modules — `module "model"`, `module "fn_flag"`, `module "fn_freeze"`, `module "fn_notify"`, `module "tool_flag"`, `module "tool_freeze"`, `module "tool_notify"`, `module "agent"`, `module "profiles"`, `module "detect"`:

```hcl
  count            = var.deploy_flink_pipeline ? 1 : 0
```

Do NOT add `count` to `module "tbl_transactions"`, `module "tbl_user_logins"`, `module "tbl_account_changes"`, or `module "tbl_fraud_alerts"` (the tables stay in both modes), and do NOT add it to `confluent_flink_connection.bedrock` or `confluent_flink_artifact.tools`.

Note: no `depends_on` or interpolation elsewhere references `module.model/fn_*/tool_*/agent/profiles/detect` outputs, so turning these into count-lists needs no `[0]` fixups. (Verified in Step 4.)

- [ ] **Step 3: Create `terraform/workshop.tfvars.example`**

```hcl
# Workshop mode preset. Copy to workshop.tfvars, fill in your 4 credentials, then:
#   terraform init && terraform apply -var-file=workshop.tfvars
# This provisions infrastructure + the 4 tables + the Bedrock connection + the tools JAR,
# but NOT the AI pipeline statements — participants create those by hand in the Flink UI
# (model, functions, tools, agent, windowing, detection). See ../WORKSHOP.md.

confluent_cloud_api_key    = "<your-confluent-cloud-api-key>"
confluent_cloud_api_secret = "<your-confluent-cloud-api-secret>"
aws_access_key_id          = "<your-aws-access-key-id>"
aws_secret_access_key      = "<your-aws-secret-access-key>"

deploy_flink_pipeline = false
```

- [ ] **Step 4: Verify no gated module is referenced elsewhere**

Run: `grep -rnE 'module\.(model|fn_flag|fn_freeze|fn_notify|tool_flag|tool_freeze|tool_notify|agent|profiles|detect)\b' terraform/`
Expected: matches appear ONLY inside `terraform/flink.tf` `depends_on` lists of the gated modules themselves (which are all present/absent together). No match in `outputs.tf`, `connect.tf`, or `main.tf`. If a match appears outside the gated set, that reference needs a `one(...)`/`[0]` fixup — stop and reconsider.

- [ ] **Step 5: Format and validate**

Run: `cd terraform && terraform fmt -recursive && terraform validate`
Expected: `fmt` prints no filenames (already formatted); `validate` prints "Success! The configuration is valid."

- [ ] **Step 6: Commit**

```bash
git add terraform/variables.tf terraform/flink.tf terraform/workshop.tfvars.example
git commit -m "$(cat <<'EOF'
Add deploy_flink_pipeline flag to gate the AI pipeline for workshop mode

Default true keeps the one-command demo unchanged. When false, the 10
pipeline statements (model, functions, tools, agent, windowing, detect)
are skipped so workshop participants run them by hand in the Flink UI;
infra + 4 tables + Bedrock connection + tools JAR still deploy.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `flink_catalog` / `flink_database` outputs

Give the manual SQL its two fill-in values (the Flink UI dropdowns show display names, not ids).

**Files:**
- Modify: `terraform/outputs.tf` (append two outputs)

**Interfaces:**
- Produces: outputs `flink_catalog` (environment display name) and `flink_database` (cluster display name), consumed as instructions in `WORKSHOP.md` Task 5.

- [ ] **Step 1: Append the two outputs to `terraform/outputs.tf`**

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

- [ ] **Step 2: Format and validate**

Run: `cd terraform && terraform fmt -recursive && terraform validate`
Expected: no `fmt` diff; `validate` → "Success! The configuration is valid."

- [ ] **Step 3: Commit**

```bash
git add terraform/outputs.tf
git commit -m "$(cat <<'EOF'
Output flink_catalog and flink_database for manual workshop SQL

The Flink workspace catalog/database dropdowns show display names, not
ids; surface them so participants can select the right context.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Rename `demo.md` → `WALKTHROUGH.md`

Free the `DEMO.md` name (case collision on macOS/APFS) and fix inbound links. Content of the tour is unchanged.

**Files:**
- Rename: `demo.md` → `WALKTHROUGH.md` (via `git mv`)
- Modify: `README.md` (link to `demo.md` → will be superseded in Task 5; leave for now OR update)
- Modify: `CLAUDE.md` (references to `demo.md`)

**Interfaces:**
- Produces: `WALKTHROUGH.md` (the presenter Stream Lineage tour); `DEMO.md` (Task 4) will link to it.

- [ ] **Step 1: Rename with git**

Run: `git mv demo.md WALKTHROUGH.md`

- [ ] **Step 2: Update the H1 wording (optional consistency)**

The current H1 is `# Demo Walkthrough — Real-Time Fraud Detection with Confluent Intelligence Streaming Agents` — leave as-is (it does not reference the filename). No content edit needed inside the file; its `images/demo/…` paths still resolve.

- [ ] **Step 3: Update `CLAUDE.md` references to `demo.md`**

Find them: `grep -n 'demo\.md' CLAUDE.md`
Replace each `demo.md` with `WALKTHROUGH.md` in the two known spots:
- Repository map row: change `` `demo.md` `` and its text so it reads: `` | `WALKTHROUGH.md` | ~15-min presenter walkthrough (Stream Lineage → Flink job → dashboard); screenshots in `images/demo/`. Linked from `DEMO.md`. | ``
- Any prose mention of `demo.md` → `WALKTHROUGH.md`.

(README's `demo.md` link is handled in Task 5 when README is restructured — if any `demo.md` reference remains in README after this task, it will be fixed there.)

- [ ] **Step 4: Verify the rename and no dangling lowercase refs outside README**

Run: `test -f WALKTHROUGH.md && ! test -f demo.md && echo OK` then `grep -rn 'demo\.md' CLAUDE.md WALKTHROUGH.md || echo "no demo.md refs in CLAUDE.md/WALKTHROUGH.md"`
Expected: `OK`, and no `demo.md` references in `CLAUDE.md` or `WALKTHROUGH.md`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
Rename demo.md to WALKTHROUGH.md to free the DEMO.md name

DEMO.md and demo.md collide on case-insensitive filesystems (macOS APFS).
WALKTHROUGH.md is the presenter Stream Lineage tour; DEMO.md (next) will
carry the demo deploy steps and link to it. Updates CLAUDE.md references.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Create `DEMO.md` (demo path deploy steps)

Move the one-command Quick Start out of README into its own path doc that links onward to the walkthrough.

**Files:**
- Create: `DEMO.md`

**Interfaces:**
- Consumes: `WALKTHROUGH.md` (Task 3), the `terraform.tfvars.example` flow (unchanged).
- Produces: `DEMO.md`, linked from `README.md` (Task 5).

- [ ] **Step 1: Create `DEMO.md`**

Write the demo path doc. Use the current README "Quick Start" steps verbatim in intent (clone → 4 creds → `terraform init && terraform apply` → install deps → run producer → run dashboard), then link to the walkthrough:

```markdown
# 🎬 Demo Path — One-Command Deploy

The fastest way to see the system: Terraform provisions **everything** — infrastructure
*and* the full Flink AI pipeline — and you just run the producer and dashboard. For the
hands-on version where you build the AI pipeline yourself, see [`WORKSHOP.md`](WORKSHOP.md).

> Complete the shared [Prerequisites](README.md#prerequisites) first (Confluent Cloud +
> AWS Bedrock credentials, Claude model access in `us-east-1`, and the required tools).

## 1. Clone the repository

```bash
git clone https://github.com/confluentinc/demo-confluent-fraud-agent.git && cd demo-confluent-fraud-agent
```

## 2. Provide your four credentials

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars — Confluent Cloud key/secret + AWS Bedrock IAM key/secret
```

These four values are the **only** inputs. Region (`us-east-1`), the Claude model, resource
names, and sizing are all preset.

## 3. Deploy everything

```bash
terraform init && terraform apply
```

This provisions the environment, Kafka cluster, Schema Registry, Flink compute pool, the
Bedrock connection, the model, the UDF tools (JAR upload + functions + tools), the agent,
and the detection query — and writes a ready-to-use `.env` for the local apps.

## 4. Install the Python dependencies

```bash
cd .. && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
```

On Windows (PowerShell): `cd .. ; python -m venv venv ; venv\Scripts\Activate.ps1 ; pip install -r requirements.txt`

## 5. Start the producer

```bash
python producer/generate_events.py
```

## 6. Start the dashboard

In a second terminal (activate the venv first), launch the UI at http://localhost:8501:

```bash
streamlit run dashboard/app.py
```

Watch fraud alerts appear in real time — the injected scenarios surface as high-risk alerts
(scores ~75–95) with `freeze_account` / `flag_transaction` actions and the flagged
transaction ids.

> [!NOTE]
> The dashboard reads from `latest`, so keep the producer running and allow ~1 minute for
> the first window-firing batch of alerts. Inspect the running statements and tables in the
> [Flink workspace](https://confluent.cloud/go/flink).

## Next: the guided walkthrough

Deployed and running? Follow the **[guided walkthrough → `WALKTHROUGH.md`](WALKTHROUGH.md)** —
a ~15-minute, screenshot-by-screenshot tour through **Stream Lineage** to the live dashboard.

## Cleanup

```bash
cd terraform && terraform destroy
```
```

- [ ] **Step 2: Verify links resolve**

Run: `grep -oE '\]\(([A-Za-z0-9_./#-]+)\)' DEMO.md`
Expected: references to `WORKSHOP.md`, `README.md#prerequisites`, `WALKTHROUGH.md` — confirm each target file exists (`WORKSHOP.md` is created in Task 5; acceptable forward reference within this branch).

- [ ] **Step 3: Commit**

```bash
git add DEMO.md
git commit -m "$(cat <<'EOF'
Add DEMO.md with the one-command demo deploy steps

Extracts the Quick Start out of the README into a dedicated demo-path doc
that links onward to the WALKTHROUGH.md guided tour.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Restructure `README.md` into a landing page

Turn README into the use-case + two-paths entry point, keeping shared reference sections.

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: `DEMO.md` (Task 4), `WORKSHOP.md` (Task 6 — forward reference on the same branch).
- Produces: the "Choose your path" landing page.

- [ ] **Step 1: Keep the top of README unchanged**

Keep lines 1–36 as-is: the title, the badge, the dashboard image, the intro paragraphs, the 4-stage "What happens" table, and the Stream Lineage image.

- [ ] **Step 2: Keep the `## Prerequisites` section unchanged**

Keep the entire existing `## Prerequisites` section (required accounts/credentials, required tools, the install-commands `<details>` block). Both paths link here.

- [ ] **Step 3: Replace the `## 🚀 Quick Start` and `## 🎬 Demo walkthrough` sections with a "Choose your path" section**

Delete the current `## 🚀 Quick Start` section (the 6 numbered steps) and the `## 🎬 Demo walkthrough` section, and insert in their place:

```markdown
## Choose your path

This repo can be run two ways. Both start from the same [Prerequisites](#prerequisites) above.

| Path | What you do | Time | Guide |
|------|-------------|------|-------|
| **🎬 Demo** | One `terraform apply` deploys **everything** — infra *and* the full Flink AI pipeline. You just run the producer and dashboard. | ~15 min | **[DEMO.md](DEMO.md)** |
| **🛠️ Self-service workshop** | Terraform deploys infra + the 4 tables; **you build the AI pipeline by hand** in the Flink UI (model → functions → tools → agent → windowing → detection). | ~1 hour | **[WORKSHOP.md](WORKSHOP.md)** |

**New here or short on time?** Take the [Demo path](DEMO.md). **Want to learn how Streaming
Agents are built, statement by statement?** Take the [Workshop path](WORKSHOP.md).
```

- [ ] **Step 4: Keep the shared reference sections**

Keep `## Directory Structure`, `## How the agent's tools work`, `## Troubleshooting`,
`## Cleanup`, and `## Sign up for early access…` as-is. In `## Directory Structure`, add the
new docs to the tree so it reflects reality — insert these lines in the file listing:

```
├── DEMO.md                          # Demo path — one-command deploy
├── WORKSHOP.md                      # Self-service workshop — manual Flink SQL
├── WALKTHROUGH.md                   # Guided post-deploy Stream Lineage tour
```

- [ ] **Step 5: Verify no stale `demo.md` link and both paths are linked**

Run: `grep -n 'demo\.md' README.md || echo "no lowercase demo.md refs"` then `grep -nE 'DEMO\.md|WORKSHOP\.md' README.md`
Expected: no `demo.md` refs; both `DEMO.md` and `WORKSHOP.md` are linked.

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
Restructure README into a landing page with two paths

Use-case + architecture up top, shared Prerequisites, then a "Choose your
path" table linking to DEMO.md (one-command) and WORKSHOP.md (self-service).
Quick Start moved to DEMO.md; directory tree updated.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Write `WORKSHOP.md` (the workshop path with inline SQL)

The core deliverable: prerequisites gate, workshop deploy, catalog/db setup, the 10 statements inline in order, apps, cleanup. SQL bodies copied verbatim from `flink.tf` with the two documented substitutions.

**Files:**
- Create: `WORKSHOP.md`
- Reference (read, do not modify): `terraform/flink.tf` (source of the SQL bodies)

**Interfaces:**
- Consumes: outputs `tools_artifact_id`, `flink_catalog`, `flink_database` (Tasks 1–2); `workshop.tfvars.example` (Task 1).

- [ ] **Step 1: Write the WORKSHOP.md skeleton (prereqs → deploy → context)**

Create `WORKSHOP.md` starting with:

```markdown
# 🛠️ Self-Service Workshop — Build the Streaming Agent by Hand

In this workshop you deploy the Confluent Cloud infrastructure with Terraform, then build the
**real-time fraud detection AI pipeline yourself** — running each Flink SQL statement in the
Confluent Cloud UI: the model, the function tools, the agent, the windowing, and the detection
query. Terraform creates the cluster, the Bedrock connection, the tools JAR, and the 4 tables
(so the producer can start immediately); **you create everything else**.

> The one-command auto-deployed version is the [Demo path](DEMO.md). This workshop is the
> hands-on version.

**Time:** ~1 hour of hands-on work — *if* the prerequisites below are done in advance.

## 0. Before you arrive (prerequisites — do these ahead of time)

These are **not** part of the timed hour. Complete them before the workshop:

- [ ] **Confluent Cloud account** + a **Cloud resource management** API key & secret.
- [ ] **AWS account with an IAM user** that has a long-lived access key & secret and the
      `bedrock:InvokeModel` permission.
- [ ] **Claude model access enabled in Amazon Bedrock**, region **`us-east-1`** — enable
      Anthropic Claude on the **Model access** page. Approval can lag, so do this early;
      without it, `AI_RUN_AGENT` fails with AccessDenied and no alerts appear.
- [ ] **Tools installed:** [Terraform](https://github.com/hashicorp/terraform),
      [Git](https://git-scm.com/), [Python 3.11+](https://www.python.org/downloads/).

## 1. Deploy the infrastructure (~8–12 min)

```bash
git clone https://github.com/confluentinc/demo-confluent-fraud-agent.git && cd demo-confluent-fraud-agent/terraform
cp workshop.tfvars.example workshop.tfvars
# edit workshop.tfvars — fill in your 4 credentials (deploy_flink_pipeline is already false)
terraform init && terraform apply -var-file=workshop.tfvars
```

This provisions the environment, Kafka cluster, Schema Registry, Flink compute pool, the
Bedrock connection, the tools JAR artifact, and the **4 tables** — but **not** the AI
pipeline. It also writes a ready-to-use `.env` for the producer and dashboard.

When it finishes, note these outputs (you'll paste them into the SQL below):

```bash
terraform output tools_artifact_id   # -> paste into the CREATE FUNCTION statements
terraform output flink_catalog       # -> the workspace Catalog to select
terraform output flink_database      # -> the workspace Database to select
```

## 2. Open the Flink workspace and set your context

Open the [Flink workspace](https://confluent.cloud/go/flink). Set the **Catalog** to your
`flink_catalog` value and the **Database** to your `flink_database` value. All the statements
below use unqualified names, so this context must be set first.
```

- [ ] **Step 2: Add the pipeline section header + statements 1–4 (model + functions)**

Append. The `CREATE MODEL` body is copied from `flink.tf` `module.model`; the 3 `CREATE
FUNCTION` bodies from `module.fn_flag/fn_freeze/fn_notify` with `confluent-artifact://${confluent_flink_artifact.tools.id}` replaced by `confluent-artifact://<TOOLS_ARTIFACT_ID>`:

````markdown
## 3. Build the pipeline — run these statements in order (~15–25 min)

Run each statement below in the workspace, one at a time, waiting for each to finish before
the next. Statements 1–8 are DDL and reach **COMPLETED** in a few seconds; statements 9 and
10 are continuous and go **RUNNING**.

### 3.1 Register the model

Wires up the Bedrock Claude model behind a callable name.

```sql
CREATE MODEL `fraud_model`
INPUT (`prompt` STRING)
OUTPUT (`response` STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = 'bedrock-fraud-connection',
  'bedrock.params.max_tokens' = '8192'
);
```

### 3.2–3.4 Create the three function tools

These map the pre-uploaded UDF JAR classes to Flink functions. **Replace
`<TOOLS_ARTIFACT_ID>` in all three with your `tools_artifact_id` output.**

```sql
CREATE FUNCTION `flag_transaction`
AS 'io.confluent.frauddemo.FlagTransaction'
USING JAR 'confluent-artifact://<TOOLS_ARTIFACT_ID>';
```

```sql
CREATE FUNCTION `freeze_account`
AS 'io.confluent.frauddemo.FreezeAccount'
USING JAR 'confluent-artifact://<TOOLS_ARTIFACT_ID>';
```

```sql
CREATE FUNCTION `notify_user`
AS 'io.confluent.frauddemo.NotifyUser'
USING JAR 'confluent-artifact://<TOOLS_ARTIFACT_ID>';
```
````

- [ ] **Step 3: Add statements 5–7 (tools)**

Append. Bodies copied verbatim from `flink.tf` `module.tool_flag/tool_freeze/tool_notify`:

````markdown
### 3.5–3.7 Wrap the functions as agent tools

Tools give the agent a name + description it can reason about when deciding to act.

```sql
CREATE TOOL `flag_transaction_tool`
USING FUNCTION `flag_transaction`
WITH (
  'type' = 'function',
  'description' = 'Flag a specific transaction as potentially fraudulent for manual review. Arguments: transaction_id, reason.'
);
```

```sql
CREATE TOOL `freeze_account_tool`
USING FUNCTION `freeze_account`
WITH (
  'type' = 'function',
  'description' = 'Temporarily freeze a user account due to suspected fraud. Arguments: user_id, reason.'
);
```

```sql
CREATE TOOL `notify_user_tool`
USING FUNCTION `notify_user`
WITH (
  'type' = 'function',
  'description' = 'Send a fraud alert notification to the user. Arguments: user_id, message.'
);
```
````

- [ ] **Step 4: Add statement 8 (agent)**

Append. Body copied verbatim from `flink.tf` `module.agent` (single-quote escaping `''`
preserved exactly, since this is Flink SQL pasted into the same UI):

````markdown
### 3.8 Create the agent

The agent binds the model + the three tools + a hardened prompt that scores each profile and
decides which tools to call.

```sql
CREATE AGENT `fraud_detection_agent`
USING MODEL `fraud_model`
USING PROMPT 'You are a real-time fraud detection analyst.

You receive a plain-text activity profile for ONE user over a short time window. It lists
the user id and that user''s recent transactions (each shown as "txn <transaction_id>: $<amount> at <merchant> ..."),
recent logins (location, device, ip), and recent account changes (field, old value, new value).

Analyze for these fraud signals:
1. Geographic impossibility: login and transaction in distant cities within minutes
2. Velocity anomalies: many transactions in a short period
3. Account takeover: email/password change followed by a large purchase
4. Unusual amounts: transactions much larger than others
5. Device/IP anomalies: new devices combined with other signals

SCORING GUIDE - use the FULL range:
- 90-100: Multiple strong signals combined (e.g. geo-impossible + account takeover + large amount)
- 70-89: One strong signal with supporting evidence (e.g. geo-impossible travel alone)
- 45-69: Suspicious patterns that need investigation (e.g. unusual amount or velocity alone)
- 20-44: Mildly unusual but likely legitimate (e.g. new device from same city)
- 0-19: Normal activity, no fraud signals detected

TOOLS - DECIDE THE SCORE FIRST, THEN ACT. Determine risk_score before calling any tool, and
only call the tools the score warrants below. Make every tool call up front. Once you call a
tool, treat it as final: never reconsider it, apologize for it, or reverse it in text.
- If risk_score >= 80: call freeze_account_tool(user_id, reason) and notify_user_tool(user_id, message)
- If risk_score 50-79: call flag_transaction_tool(transaction_id, reason) for each suspicious transaction and notify_user_tool(user_id, message)
- If risk_score 20-49: call notify_user_tool(user_id, message)
- If risk_score < 20: do NOT call any tool.
Record the tools you actually called in "actions_taken".

CRITICAL RULES:
- Copy the EXACT "user_id" string from the input. Do NOT change it.
- Copy EXACT "transaction_id" strings from the input into "flagged_transaction_ids". Do NOT invent IDs.
- If no transactions exist, set "flagged_transaction_ids" to an empty list.
- Your FINAL message must be ONLY the single JSON object below: no preamble, no commentary, no
  self-corrections, no markdown, no code fences. Put all explanation inside "reasoning", nowhere else.
{"user_id": "<copy from input>", "risk_score": <0-100 integer>, "reasoning": "<one or two sentences>", "actions_taken": ["freeze_account"|"flag_transaction"|"notify_user"], "flagged_transaction_ids": ["<copied transaction ids>"]}'
USING TOOLS `flag_transaction_tool`, `freeze_account_tool`, `notify_user_tool`
WITH (
  'max_iterations' = '6',
  'handle_exception' = 'continue',
  'max_consecutive_failures' = '5'
);
```
````

- [ ] **Step 5: Add statement 9 (the SET + windowing) with the idle-timeout callout**

Append. The `SET` replaces `module.profiles`'s `extra_properties`; the CTAS body is copied
verbatim from `module.profiles`:

````markdown
### 3.9 Build the activity profiles (windowing)

> [!IMPORTANT]
> **Run the `SET` first, in the same workspace session, before the `CREATE TABLE`.** It pins
> the watermark idle-timeout to 5s. Without it, Confluent Cloud's default "progressive
> idleness" grows the idle timeout as the statement ages and the session windows stall — you'd
> see an early burst of alerts that then dries up. (In the demo path this is set automatically
> as a statement property on the windowing job.)

```sql
SET 'sql.tables.scan.idle-timeout' = '5 s';
```

This unions the three input streams and collects each user's activity into a 3-second
event-time SESSION window — one profile per activity burst. It is materialized as its own
`activity_profiles` table so you can query it and see it in Stream Lineage.

```sql
CREATE TABLE `activity_profiles` AS
WITH `unified` AS (
  SELECT `user_id`, 'transaction' AS `event_type`, `event_time`,
         CONCAT('- txn ', `transaction_id`, ': $', CAST(`amount` AS STRING),
                ' at ', `merchant`, ' (', `merchant_category`, ') in ', `location`) AS `line`
  FROM `transactions`
  UNION ALL
  SELECT `user_id`, 'login' AS `event_type`, `event_time`,
         CONCAT('- login from ', `location`, ' via ', `device_id`, ' (ip ', `ip_address`, ')') AS `line`
  FROM `user_logins`
  UNION ALL
  SELECT `user_id`, 'account_change' AS `event_type`, `event_time`,
         CONCAT('- ', `field_changed`, ' changed from "', `old_value`, '" to "', `new_value`, '"') AS `line`
  FROM `account_changes`
)
SELECT
  `user_id`,
  `window_start`,
  `window_end`,
  CONCAT(
    'User: ', `user_id`, '\n\n',
    'Transactions:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'transaction' THEN `line` END, '\n'), '  (none)'), '\n\n',
    'Logins:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'login' THEN `line` END, '\n'), '  (none)'), '\n\n',
    'Account changes:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'account_change' THEN `line` END, '\n'), '  (none)')
  ) AS `profile_text`
FROM TABLE(
  SESSION(TABLE `unified` PARTITION BY `user_id`, DESCRIPTOR(`event_time`), INTERVAL '3' SECONDS)
)
GROUP BY `user_id`, `window_start`, `window_end`;
```
````

- [ ] **Step 6: Add statement 10 (detection) + apps + cleanup**

Append. The `INSERT` body is copied verbatim from `flink.tf` `module.detect`:

````markdown
### 3.10 Run the detection query

Runs the agent over each profile and parses its JSON verdict into the `fraud_alerts` table.
This is the statement that actually calls Bedrock — it stays **RUNNING**.

```sql
INSERT INTO `fraud_alerts`
WITH `scored` AS (
  SELECT
    `user_id`,
    CAST(`response` AS STRING) AS `raw_response`,
    REGEXP_EXTRACT(CAST(`response` AS STRING), '\{[\s\S]*\}', 0) AS `json_text`
  FROM `activity_profiles`,
  LATERAL TABLE(AI_RUN_AGENT(`fraud_detection_agent`, `profile_text`, `user_id`))
)
SELECT
  `user_id`,
  COALESCE(CAST(JSON_VALUE(`json_text`, '$.risk_score') AS INT), 0) AS `risk_score`,
  COALESCE(JSON_VALUE(`json_text`, '$.reasoning'), '') AS `reasoning`,
  COALESCE(JSON_QUERY(`json_text`, '$.actions_taken'), '[]') AS `actions_taken`,
  COALESCE(JSON_QUERY(`json_text`, '$.flagged_transaction_ids'), '[]') AS `flagged_transaction_ids`,
  `raw_response`
FROM `scored`;
```

## 4. Run the producer and dashboard (~8–12 min)

From the repo root, in two terminals (activate the venv in each):

```bash
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
python producer/generate_events.py          # terminal 1
streamlit run dashboard/app.py              # terminal 2 -> http://localhost:8501
```

The producer and dashboard use the `.env` Terraform already wrote. The dashboard reads from
`latest`, so keep the producer running and allow ~1–2 minutes for the first window-firing
batch of alerts.

You can also watch the two stages directly in the workspace:

```sql
SELECT * FROM activity_profiles;                     -- one row per user per session window
SELECT * FROM fraud_alerts WHERE risk_score >= 70;   -- the high-risk verdicts
```

## 5. Cleanup

```bash
cd terraform && terraform destroy -var-file=workshop.tfvars
```
````

- [ ] **Step 7: Verify every SQL block matches `flink.tf`**

For each statement, confirm the WORKSHOP.md body equals the `flink.tf` body modulo the two
documented substitutions. Spot-check with:

Run: `grep -c 'CREATE FUNCTION' WORKSHOP.md` → expect `3`; `grep -c 'CREATE TOOL' WORKSHOP.md` → expect `3`; `grep -c '<TOOLS_ARTIFACT_ID>' WORKSHOP.md` → expect `3`; `grep -c "sql.tables.scan.idle-timeout" WORKSHOP.md` → expect `1`; `grep -c 'CREATE AGENT' WORKSHOP.md` → expect `1`; `grep -c 'AI_RUN_AGENT' WORKSHOP.md` → expect `1`.
Then visually diff the agent prompt block and the windowing CTAS against `terraform/flink.tf` to confirm they are character-identical (aside from the substitutions).

- [ ] **Step 8: Commit**

```bash
git add WORKSHOP.md
git commit -m "$(cat <<'EOF'
Add WORKSHOP.md: self-service path with inline Flink SQL

Prerequisites gate, workshop-mode deploy, workspace context, the 10
pipeline statements in order (functions use <TOOLS_ARTIFACT_ID>; windowing
preceded by SET idle-timeout), then producer/dashboard and cleanup.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Update `CLAUDE.md` for workshop mode

Document the new mode, doc layout, and the sync constraint so future maintainers keep them aligned.

**Files:**
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: everything from Tasks 1–6.

- [ ] **Step 1: Update the Repository map**

In the Repository map table, add rows / update text so it reflects the new files:
- Add: `` | `WORKSHOP.md` | Self-service workshop path: deploy infra+tables via `deploy_flink_pipeline=false`, then run the 10 pipeline statements by hand in the Flink UI. Inline SQL must match `flink.tf`. | ``
- Add: `` | `DEMO.md` | Demo path: one-command `terraform apply` deploy steps; links to `WALKTHROUGH.md`. | ``
- Ensure the `WALKTHROUGH.md` row exists (from Task 3).
- Note README is now a landing page linking to `DEMO.md` / `WORKSHOP.md`.
- Add `terraform/workshop.tfvars.example` mention to the `terraform/` overview or map.

- [ ] **Step 2: Document the mode flag under "Key conventions & constraints"**

Add a bullet:

```markdown
- **Two deployment paths via `deploy_flink_pipeline`.** Default `true` = the one-command
  demo (deploys everything). `false` (see `terraform/workshop.tfvars.example`) = workshop
  mode: Terraform deploys infra + the Bedrock connection + tools JAR + the 4 tables, and the
  10 pipeline statement modules (`model`, `fn_*`, `tool_*`, `agent`, `profiles`, `detect`)
  are gated off with `count` so participants run them by hand from the Flink UI per
  `WORKSHOP.md`.
```

- [ ] **Step 3: Add the sync constraint**

Add a bullet under Key conventions:

```markdown
- **Keep `WORKSHOP.md` SQL in sync with `flink.tf`.** `WORKSHOP.md` embeds copies of the 10
  pipeline statements for participants to paste. When you change a statement in `flink.tf`,
  update the matching block in `WORKSHOP.md`. The only intended differences: the 3
  `CREATE FUNCTION` statements use the `<TOOLS_ARTIFACT_ID>` placeholder, and the windowing
  statement is preceded by `SET 'sql.tables.scan.idle-timeout' = '5 s';` (in `flink.tf` that
  lives in `module.profiles`'s `extra_properties`).
```

- [ ] **Step 4: Fix any remaining `demo.md` references**

Run: `grep -n 'demo\.md' CLAUDE.md || echo "clean"`
Expected: `clean` (all updated to `WALKTHROUGH.md` in Task 3 / here).

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
Document workshop mode and the two-path doc layout in CLAUDE.md

Adds the deploy_flink_pipeline convention, the WORKSHOP.md<->flink.tf sync
rule, and the updated repository map (DEMO.md / WORKSHOP.md / WALKTHROUGH.md).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Final cross-doc verification

Whole-repo consistency pass after all files exist.

**Files:** none (verification only)

- [ ] **Step 1: No case-colliding or stale filenames**

Run: `ls demo.md 2>/dev/null && echo "FAIL: demo.md still exists" || echo "OK: no demo.md"` then `ls DEMO.md WORKSHOP.md WALKTHROUGH.md README.md`
Expected: `OK: no demo.md`; the other four all listed.

- [ ] **Step 2: All internal doc links resolve**

Run: `grep -rhoE '\]\(([A-Za-z0-9_./#-]+\.md[^)]*)\)' README.md DEMO.md WORKSHOP.md WALKTHROUGH.md | grep -oE '[A-Za-z0-9_./-]+\.md' | sort -u`
Then confirm each listed `.md` file exists in the repo. Expected set includes `DEMO.md`, `WORKSHOP.md`, `WALKTHROUGH.md`, `README.md`. No `demo.md`.

- [ ] **Step 3: Terraform still clean**

Run: `cd terraform && terraform fmt -recursive -check && terraform validate`
Expected: `fmt -check` exits 0 (no diff); `validate` → "Success! The configuration is valid."

- [ ] **Step 4: Confirm the demo default is unchanged**

Run: `grep -A3 'variable "deploy_flink_pipeline"' terraform/variables.tf | grep 'default'`
Expected: `default     = true`.

- [ ] **Step 5: Final commit (if any verification fixes were needed)**

Only if Steps 1–4 required edits:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Fix cross-doc consistency for the workshop path

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- Terraform gating (spec §1) → Task 1. ✓
- New outputs (spec §2) → Task 2. ✓
- `workshop.tfvars.example` (spec §3) → Task 1 Step 3. ✓
- `WORKSHOP.md` (spec §4) → Task 6 (all 6 structure points: prereqs, deploy, context, 10 statements with placeholder + SET, apps, cleanup). ✓
- README landing page (spec §5) → Task 5. ✓
- `DEMO.md` (spec §6) → Task 4. ✓
- `demo.md` → `WALKTHROUGH.md` rename (spec §7) → Task 3. ✓
- CLAUDE.md updates + sync note (spec §8) → Task 7. ✓
- Verification (spec Testing) → per-task `fmt`/`validate`/grep + Task 8 cross-doc pass. ✓

**Placeholder scan:** No TBD/TODO. `<TOOLS_ARTIFACT_ID>` is an intentional, documented literal placeholder in the produced doc (not a plan placeholder). All SQL bodies are written out in full.

**Type/name consistency:** Variable `deploy_flink_pipeline` (bool, default true), outputs `flink_catalog`/`flink_database`/`tools_artifact_id`, module names, and the fixed SQL object names are used identically across Tasks 1, 2, 6, 7 and match `flink.tf`. Doc filenames `DEMO.md`/`WORKSHOP.md`/`WALKTHROUGH.md` are consistent across Tasks 3–8.

**Ordering note:** Tasks 4–5 forward-reference `WORKSHOP.md` (created in Task 6); acceptable because all tasks land on the same `workshop-path` branch and Task 8 verifies all links at the end.
