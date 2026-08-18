# ---------------------------------------------------------------------------
# The ONLY four values a user must provide (in terraform.tfvars).
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

variable "aws_access_key_id" {
  description = "AWS IAM user access key ID with Amazon Bedrock invoke permission."
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS IAM user secret access key."
  type        = string
  sensitive   = true
}

variable "deploy_flink_pipeline" {
  description = "Deploy the hands-on Flink AI pipeline statements (model, tools, agent, windowing, detection). true = the one-command demo. Set false for the self-service workshop, where participants run these statements by hand in the Flink UI (see WORKSHOP.md). The 3 CREATE FUNCTION statements are always deployed (they reference the uploaded JAR artifact id), so participants never handle that id."
  type        = bool
  default     = true
}
