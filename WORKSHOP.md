# 🛠️ Self-Service Workshop — Build a Fraud Streaming Agent

In this workshop you deploy the Confluent Cloud infrastructure with Terraform, then build the
**real-time fraud detection AI pipeline yourself**.

**Time:** ~1 hour of hands-on work — *if* the prerequisites below are done in advance.

## 0. Prerequisites

These are **not** part of the timed hour. Complete them before the workshop:

- [ ] **Confluent Cloud account, a payment method, and a Cloud API key & secret** — the three
      steps in [0.1](#01-set-up-confluent-cloud) below.
- [ ] **AWS account with an IAM user** that has a long-lived access key & secret and the
      `bedrock:InvokeModel` permission.
- [ ] **Claude model access enabled in Amazon Bedrock**, region **`us-east-1`** — enable
      Anthropic Claude on the **Model access** page. Approval can lag, so do this early;
      without it, `AI_RUN_AGENT` fails with AccessDenied and no alerts appear.
- [ ] **Tools installed:** [Terraform](https://github.com/hashicorp/terraform),
      [Git](https://git-scm.com/), [Python 3.11+](https://www.python.org/downloads/).

### 0.1 Set up Confluent Cloud

#### Step 1 — Create your Confluent Cloud account

1. Go to **[confluent.cloud/signup](https://confluent.cloud/signup)**.
2. Enter your **name**, **work email**, **company**, and a **password**, then accept the terms
   and click **Start Free**.
3. On first sign-in, an **onboarding wizard** asks a few questions, then offers to create your
   first cluster.

> [!WARNING]
> **Do NOT create a cluster.** Skip the **Add cluster** / **Create cluster** step — Terraform
> creates its own environment *and* cluster for you. Click the Confluent logo (top-left) to
> exit the wizard.

#### Step 2 — Add a payment method (credit card)

A card is required for Terraform to create a **Standard** cluster. Free credit is used first, so
you typically won't be charged during the workshop.

> [!TIP]
> All new sign-ups get **$400 in free credit** to use within the first 30 days — deploying this
> workshop will not get you charged.

1. Go directly to **[Billing & payment](https://confluent.cloud/settings/billing/payment)**.
2. Open the **Payment details and contacts** tab.
3. Click **Add credit card / bank account**.

<img src="images/workshop/0_step2_1.png" alt="Payment details and contacts — Add credit card / bank account" width="500">


4. Enter your **card details**, then **Save**.

#### Step 3 — Create a Cloud API key & secret

Terraform authenticates with an **org-level "Cloud resource management" API key**. This is what
you'll paste into `workshop.tfvars`.

1. Go directly to **[API keys](https://confluent.cloud/settings/api-keys)** and click **+ Add API key**.
2. On the **Create API key** form: enter a **Name** (e.g. `Workshop`), select **My account**, and
   set **Select key scope** to **Cloud resource management**.

<img src="images/workshop/0_step3_1.png" alt="Create API key — My account + Cloud resource management" width="500">


> [!NOTE]
> A service account works too, but then you must grant it the OrganizationAdmin role
> separately — "My account" already has your permissions.

3. Click **Create API key**, then **copy the Key and Secret** (the secret is shown **only once**,
   or click **Download**). These are your `confluent_cloud_api_key` and `confluent_cloud_api_secret`
   in `workshop.tfvars`.

## 1. Deploy the infrastructure (~8–12 min)

1. Clone the repo and enter the `terraform` directory:

   ```bash
   git clone https://github.com/confluentinc/demo-confluent-fraud-agent.git
   cd demo-confluent-fraud-agent/terraform
   ```

2. Create your vars file from the template:

   ```bash
   cp workshop.tfvars.example workshop.tfvars
   ```

3. Edit **`workshop.tfvars`** and fill in your 4 credentials (`deploy_flink_pipeline` is already
   set to `false`).

4. Deploy:

   ```bash
   terraform init && terraform apply -var-file=workshop.tfvars
   ```

This provisions the environment, Kafka cluster, Schema Registry, Flink compute pool, the
Bedrock connection, the tools JAR artifact, and the **4 tables** — but **not** the AI
pipeline. It also writes a ready-to-use `.env` for the producer and dashboard.

> [!NOTE]
> `terraform apply` takes **~8–12 minutes** — provisioning the Kafka cluster is the slow part.

When it finishes, note these outputs — they're the workspace context you'll select in the
Flink UI:

```bash
terraform output flink_catalog       # -> the workspace Catalog to select
terraform output flink_database      # -> the workspace Database to select
```

## 2. Connect the model — give the agent a brain

Now we build the fraud-detection pipeline.

Fraud rarely fits fixed if/else rules — catching it takes judgment. So the first thing you do
is give the pipeline access to a large language model. This statement registers that model as
`fraud_model` so Flink SQL can call it like any other function.

Run this — and every statement in the sections that follow — in the Flink UI:

1. Navigate to the [Flink UI](https://confluent.cloud/go/flink) and select the **environment**
   Terraform created (`fraud-agent-env-…`).
2. Open a **SQL workspace**.
3. Set the **Catalog** to your `flink_catalog` output and the **Database** to your
   `flink_database` output.

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

On its own the model just "thinks" — you'll give it actions and a job in the next sections.

## 3. Define the actions — give the agent hands

A fraud analyst that can only *think* isn't useful; it has to *act* — flag a suspicious
transaction, freeze a compromised account, or warn the customer. That takes two layers: a
**function** (the actual action, implemented in the UDF JAR) and a **tool** (the plain-English
*description* the agent reads to decide *when* to call that action).

The three **functions** — `flag_transaction`, `freeze_account`, `notify_user` — were already
created for you by Terraform (they reference the uploaded JAR by its artifact id). Your job is
to expose each one as a **tool**.

Here's the first tool, wrapping the `flag_transaction` function:

```sql
CREATE TOOL `flag_transaction_tool`
USING FUNCTION `flag_transaction`
WITH (
  'type' = 'function',
  'description' = 'Flag a specific transaction as potentially fraudulent for manual review. Arguments: transaction_id, reason.'
);
```

> [!NOTE]
> **Your turn.** Complete the two tools below by filling in the `<function-name>` each one
> wraps, following the pattern above.

```sql
CREATE TOOL `freeze_account_tool`
USING FUNCTION `<function-name>`
WITH (
  'type' = 'function',
  'description' = 'Temporarily freeze a user account due to suspected fraud. Arguments: user_id, reason.'
);
```

```sql
CREATE TOOL `notify_user_tool`
USING FUNCTION `<function-name>`
WITH (
  'type' = 'function',
  'description' = 'Send a fraud alert notification to the user. Arguments: user_id, message.'
);
```

<details>
<summary>Hint — how do I find the function names?</summary>

Terraform already created the functions for you. List them with:

```sql
SHOW USER FUNCTIONS;
```
</details>

## 4. Build each user's activity profile — turn events into a story

A fraud analyst needs one user's recent behavior in a single view, but events arrive as three
separate raw streams (transactions, logins, account changes). This statement stitches them
together: for each user it gathers a short burst of activity into a single, human-readable
profile. A **session window** groups events that happen close together and closes when the user
goes quiet, so a fraud burst is never split in two. This statement runs **continuously** (it
stays **RUNNING**).

This unions the three input streams and collects each user's activity into a 3-second
event-time SESSION window — one profile per activity burst. It is materialized as its own
`activity_profiles` table so you can query it and see it in Stream Lineage. Run the whole block
below together:

```sql
SET 'client.statement-name' = 'create-activity-profiles';
SET 'sql.tables.scan.idle-timeout' = '5 s';
SET 'sql.tables.scan.startup.mode' = 'latest-offset';

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

> [!IMPORTANT]
> The two `SET`s in the block above matter:
> - **`idle-timeout = '5 s'`** keeps the session-window watermark advancing — without it alerts start, then dry up.
> - **`startup.mode = 'latest-offset'`** reads only new events. Everyone shares one Bedrock account, so replaying topic history would spike the shared quota.

> [!NOTE]
> This is a continuous statement — keep this cell **RUNNING** for the whole workshop.

## 5. Assemble the fraud analyst — create the agent

Now combine the brain, the hands, and a **job description**. The prompt is where the use-case
logic lives: it tells the agent how to score risk from 0–100, which tools to call at each score
band (freeze + notify when risk is high, flag + notify when medium, and so on), and to return a
single strict-JSON verdict you can store. Read the prompt below — it *is* the fraud policy.

> [!NOTE]
> **Your turn.** Fill in the `<model-name>` the agent runs on.

```sql
CREATE AGENT `fraud_detection_agent`
USING MODEL `<model-name>`
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

<details>
<summary>Hint — how do I find the model name?</summary>

You created the model in section 2. List your models with:

```sql
SHOW MODELS;
```
</details>

The agent is now **created but not running** — nothing calls it yet. In the next step we put it
to work.

## 6. Detect fraud in real time — put the analyst to work

This is the payoff. For **every** activity profile that appears, this statement calls the agent,
lets it score and act, and writes the verdict — risk score, reasoning, actions taken, and
flagged transaction ids — into the `fraud_alerts` table. It runs **continuously**: each new
burst of user activity is analyzed within seconds. This is also the statement that actually
calls Bedrock, so leave it **RUNNING**.

```sql
SET 'client.statement-name' = 'detect-fraud';

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

> [!NOTE]
> This is a continuous statement — keep this cell **RUNNING** for the whole workshop.

## 7. Generate activity and watch alerts (~8–12 min)

Your pipeline is live but idle — no events are flowing yet. Now you'll simulate customer
activity and watch the agent score it in real time. Two local apps do this, both reading the
`.env` Terraform already wrote (no config needed):

- the **producer** — stands in for your customers, streaming synthetic transactions, logins,
  and account changes (~80% normal, ~20% fraud) into your topics;
- the **dashboard** — the fraud analyst's screen, showing the agent's alerts as they land.

**1. Install dependencies** (once, from the repo root):

```bash
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
```

**2. Start the producer** in this terminal:

```bash
python producer/generate_events.py
```

**3. Start the dashboard** in a **second** terminal (activate the venv there first):

```bash
source venv/bin/activate
streamlit run dashboard/app.py   # open http://localhost:8501
```

Give it ~1–2 minutes: as each user's activity burst closes its session window, the agent scores
it and high-risk cases surface as alerts. Keep the producer running — the dashboard shows alerts
generated from here on.

You can also watch the two stages directly in the workspace:

```sql
SELECT * FROM activity_profiles;                     -- one row per user per session window
SELECT * FROM fraud_alerts WHERE risk_score >= 70;   -- the high-risk verdicts
```

## 8. Cleanup

```bash
cd terraform && terraform destroy -var-file=workshop.tfvars
```
