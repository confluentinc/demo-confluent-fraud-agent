# ---------------------------------------------------------------------------
# The only values a user must provide: the Confluent Cloud key/secret, plus the
# AWS Bedrock key/secret when llm_provider = "bedrock". `uv run deploy` writes
# them to terraform.tfvars from credentials.env.
# Everything else (region, model, resource names, sizes) is fixed in locals
# below so running this demo requires no other decisions.
# ---------------------------------------------------------------------------

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management)."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret."
  type        = string
  sensitive   = true
}

variable "llm_provider" {
  description = "Backs fraud_model: 'confluent' (CSP Hosted Flink Model Inference — no AWS keys, the default) or 'bedrock' (your own AWS Bedrock keys; `uv run deploy --bedrock`)."
  type        = string
  default     = "confluent"
  validation {
    condition     = contains(["bedrock", "confluent"], var.llm_provider)
    error_message = "llm_provider must be 'bedrock' or 'confluent'."
  }
}

variable "csp_hosted_model" {
  description = "Model id for llm_provider = 'confluent'. Case-sensitive: the model service matches it exactly."
  type        = string
  default     = "anthropic.claude-sonnet-4-6"
}

# --- AWS Bedrock (unused when llm_provider = 'confluent') ---

variable "aws_bedrock_access_key" {
  description = "AWS IAM user access key ID with Amazon Bedrock invoke permission."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_bedrock_secret_key" {
  description = "AWS IAM user secret access key."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_session_token" {
  description = "AWS session token, only for temporary (ASIA...) credentials. Leave empty for a long-lived IAM user key."
  type        = string
  sensitive   = true
  default     = ""
}

variable "deploy_flink_pipeline" {
  description = "Deploy the hands-on Flink AI pipeline statements (model, tools, agent, windowing, detection). true = the one-command demo. Set false for the self-service workshop, where participants run these statements by hand in the Flink UI (see WORKSHOP.md). The 3 CREATE FUNCTION statements are always deployed (they reference the uploaded JAR artifact id), so participants never handle that id."
  type        = bool
  default     = true
}
