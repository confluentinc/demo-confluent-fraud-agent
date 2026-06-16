# ---------------------------------------------------------------------------
# Bedrock connection (IAM user creds, fixed region). The Claude model id is
# encoded in the endpoint URL (see locals in main.tf).
# ---------------------------------------------------------------------------
resource "confluent_flink_connection" "bedrock" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  display_name   = "bedrock-fraud-connection"
  type           = "BEDROCK"
  endpoint       = local.bedrock_endpoint
  aws_access_key = var.aws_access_key_id
  aws_secret_key = var.aws_secret_access_key

  depends_on = [
    confluent_api_key.flink,
    confluent_role_binding.app_manager_env_admin,
  ]
}

# ---------------------------------------------------------------------------
# Pre-built UDF tools JAR (committed to the repo) uploaded as a Flink artifact.
# Demo users need no Java/Maven toolchain — Terraform just uploads the binary.
# ---------------------------------------------------------------------------
resource "confluent_flink_artifact" "tools" {
  display_name   = "${local.prefix}-tools-${random_id.suffix.hex}"
  cloud          = local.cloud_provider
  region         = local.aws_region
  content_format = "JAR"
  artifact_file  = "${path.module}/../tools-udf/target/fraud-tools.jar"
  environment {
    id = confluent_environment.main.id
  }
}

# Shared args for every Flink statement (passed to the flink-statement module).
locals {
  flink_common = {
    organization_id  = data.confluent_organization.main.id
    environment_id   = confluent_environment.main.id
    compute_pool_id  = confluent_flink_compute_pool.main.id
    principal_id     = confluent_service_account.app_manager.id
    rest_endpoint    = data.confluent_flink_region.main.rest_endpoint
    flink_api_key    = confluent_api_key.flink.id
    flink_api_secret = confluent_api_key.flink.secret
    catalog          = local.catalog
    database         = local.database
  }
}

# ----------------------------- Source tables -------------------------------
# Created by Flink so the topics + Avro value schemas exist before the producer
# runs and before the detection statement is deployed. `timestamp` is epoch ms;
# event_time is a computed time attribute used for the session window.

module "tbl_transactions" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-transactions-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `transactions` (
      `user_id` STRING NOT NULL,
      `transaction_id` STRING NOT NULL,
      `amount` DOUBLE NOT NULL,
      `merchant` STRING NOT NULL,
      `merchant_category` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_user_logins" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-user-logins-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `user_logins` (
      `user_id` STRING NOT NULL,
      `ip_address` STRING NOT NULL,
      `device_id` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_account_changes" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-account-changes-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `account_changes` (
      `user_id` STRING NOT NULL,
      `field_changed` STRING NOT NULL,
      `old_value` STRING NOT NULL,
      `new_value` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

# Sink table the detection statement writes alerts into; the dashboard consumes it.
module "tbl_fraud_alerts" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-fraud-alerts-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `fraud_alerts` (
      `user_id` STRING NOT NULL,
      `risk_score` INT NOT NULL,
      `reasoning` STRING NOT NULL,
      `actions_taken` STRING NOT NULL,
      `flagged_transaction_ids` STRING NOT NULL,
      `raw_response` STRING NOT NULL
    );
  EOT
}

# ------------------------------- Model -------------------------------------
module "model" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-model-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE MODEL `fraud_model`
    INPUT (`prompt` STRING)
    OUTPUT (`response` STRING)
    WITH (
      'provider' = 'bedrock',
      'task' = 'text_generation',
      'bedrock.connection' = '${confluent_flink_connection.bedrock.display_name}',
      'bedrock.params.max_tokens' = '8192'
    );
  EOT
  depends_on       = [confluent_flink_connection.bedrock]
}

# ------------------------ Functions (from JAR) -----------------------------
module "fn_flag" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-flag-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `flag_transaction`
    AS 'io.confluent.frauddemo.FlagTransaction'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

module "fn_freeze" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-freeze-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `freeze_account`
    AS 'io.confluent.frauddemo.FreezeAccount'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

module "fn_notify" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-notify-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `notify_user`
    AS 'io.confluent.frauddemo.NotifyUser'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

# --------------------------------- Tools -----------------------------------
module "tool_flag" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-flag-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `flag_transaction_tool`
    USING FUNCTION `flag_transaction`
    WITH (
      'type' = 'function',
      'description' = 'Flag a specific transaction as potentially fraudulent for manual review. Arguments: transaction_id, reason.'
    );
  EOT
  depends_on       = [module.fn_flag]
}

module "tool_freeze" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-freeze-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `freeze_account_tool`
    USING FUNCTION `freeze_account`
    WITH (
      'type' = 'function',
      'description' = 'Temporarily freeze a user account due to suspected fraud. Arguments: user_id, reason.'
    );
  EOT
  depends_on       = [module.fn_freeze]
}

module "tool_notify" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-notify-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `notify_user_tool`
    USING FUNCTION `notify_user`
    WITH (
      'type' = 'function',
      'description' = 'Send a fraud alert notification to the user. Arguments: user_id, message.'
    );
  EOT
  depends_on       = [module.fn_notify]
}

# --------------------------------- Agent -----------------------------------
module "agent" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-agent-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
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

    TOOLS - call them to act on your assessment, then record what you did in "actions_taken":
    - If risk_score >= 80: call freeze_account_tool(user_id, reason) and notify_user_tool(user_id, message)
    - If risk_score 50-79: call flag_transaction_tool(transaction_id, reason) for each suspicious transaction and notify_user_tool(user_id, message)
    - If risk_score 20-49: call notify_user_tool(user_id, message)
    - If risk_score < 20: take no action

    CRITICAL RULES:
    - Copy the EXACT "user_id" string from the input. Do NOT change it.
    - Copy EXACT "transaction_id" strings from the input into "flagged_transaction_ids". Do NOT invent IDs.
    - If no transactions exist, set "flagged_transaction_ids" to an empty list.
    - After using any tools, respond with ONLY a single valid JSON object and no other text, no markdown, no code fences:
    {"user_id": "<copy from input>", "risk_score": <0-100 integer>, "reasoning": "<one or two sentences>", "actions_taken": ["freeze_account"|"flag_transaction"|"notify_user"], "flagged_transaction_ids": ["<copied transaction ids>"]}'
    USING TOOLS `flag_transaction_tool`, `freeze_account_tool`, `notify_user_tool`
    WITH (
      'max_iterations' = '6'
    );
  EOT
  depends_on = [
    module.model,
    module.tool_flag,
    module.tool_freeze,
    module.tool_notify,
  ]
}

# ----------------------- Activity profiles (windowing) ---------------------
# Unions the 3 differently-shaped streams into a common (user_id, event_type,
# event_time, line) shape, then builds one per-user activity profile per 3s
# event-time SESSION window. Materialized as its own table/topic so it's
# queryable (SELECT * FROM activity_profiles) and shows up as a node in Stream
# Lineage between the source topics and the agent.
module "profiles" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  # Pin a small fixed watermark idle-timeout. Confluent Cloud's default
  # "progressive idleness" grows the idle timeout with statement age (up to 5
  # min), which stalls the session windows over time when the producer leaves
  # some partitions idle between cycles. A fixed 5s keeps the watermark
  # advancing so windows close and profiles flow continuously.
  extra_properties = {
    "sql.tables.scan.idle-timeout" = "5 s"
  }
  statement_name = "create-activity-profiles-${random_id.suffix.hex}"
  statement      = <<-EOT
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
  EOT
  depends_on = [
    module.tbl_transactions,
    module.tbl_user_logins,
    module.tbl_account_changes,
  ]
}

# ----------------------- Detection (agent) ---------------------------------
# Reads each per-user activity profile, runs the Streaming Agent on it, and
# parses the agent's JSON verdict into the fraud_alerts columns.
module "detect" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "detect-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
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
  EOT
  depends_on = [
    module.agent,
    module.profiles,
    module.tbl_fraud_alerts,
  ]
}
