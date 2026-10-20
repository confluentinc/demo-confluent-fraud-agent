output "environment_id" {
  value       = confluent_environment.main.id
  description = "Confluent Cloud environment id"
}

output "kafka_cluster_id" {
  value       = confluent_kafka_cluster.standard.id
  description = "Kafka cluster id"
}

output "bootstrap_servers" {
  value       = replace(confluent_kafka_cluster.standard.bootstrap_endpoint, "SASL_SSL://", "")
  description = "Kafka bootstrap servers (host:port) for the producer/dashboard"
}

output "schema_registry_url" {
  value       = data.confluent_schema_registry_cluster.sr.rest_endpoint
  description = "Schema Registry REST endpoint"
}

output "flink_compute_pool_id" {
  value       = confluent_flink_compute_pool.main.id
  description = "Flink compute pool id"
}

output "tools_artifact_id" {
  value       = confluent_flink_artifact.tools.id
  description = "Flink artifact id for the UDF tools JAR"
}

output "flink_workspace_url" {
  value       = "https://confluent.cloud/go/flink"
  description = "Open the Flink workspace to inspect statements/tables"
}

output "dotenv_path" {
  value       = local_file.dotenv.filename
  description = "Path to the generated .env consumed by the producer and dashboard"
}

output "flink_catalog" {
  value       = confluent_environment.main.display_name
  description = "Flink catalog to select in the workspace (environment display name)"
}

output "flink_database" {
  value       = confluent_kafka_cluster.standard.display_name
  description = "Flink database to select in the workspace (cluster display name)"
}

# ---------------------------------------------------------------------------
# Outputs read by the shared `uv run` scripts (setup-mcp, deploy). Names match
# terraform/core in the other streaming-agent repos so the scripts stay shared.
# ---------------------------------------------------------------------------

output "confluent_organization_id" {
  value = data.confluent_organization.main.id
}

output "confluent_environment_id" {
  value = confluent_environment.main.id
}

output "confluent_environment_display_name" {
  value = confluent_environment.main.display_name
}

output "confluent_kafka_cluster_id" {
  value = confluent_kafka_cluster.standard.id
}

output "confluent_kafka_cluster_display_name" {
  value = confluent_kafka_cluster.standard.display_name
}

output "confluent_kafka_cluster_bootstrap_endpoint" {
  value = confluent_kafka_cluster.standard.bootstrap_endpoint
}

output "confluent_kafka_cluster_rest_endpoint" {
  value = confluent_kafka_cluster.standard.rest_endpoint
}

output "confluent_schema_registry_rest_endpoint" {
  value = data.confluent_schema_registry_cluster.sr.rest_endpoint
}

output "confluent_flink_compute_pool_id" {
  value = confluent_flink_compute_pool.main.id
}

output "confluent_flink_rest_endpoint" {
  value = data.confluent_flink_region.main.rest_endpoint
}

output "app_manager_service_account_id" {
  value = confluent_service_account.app_manager.id
}

output "app_manager_kafka_api_key" {
  value     = confluent_api_key.kafka.id
  sensitive = true
}

output "app_manager_kafka_api_secret" {
  value     = confluent_api_key.kafka.secret
  sensitive = true
}

output "app_manager_schema_registry_api_key" {
  value     = confluent_api_key.schema_registry.id
  sensitive = true
}

output "app_manager_schema_registry_api_secret" {
  value     = confluent_api_key.schema_registry.secret
  sensitive = true
}

output "app_manager_flink_api_key" {
  value     = confluent_api_key.flink.id
  sensitive = true
}

output "app_manager_flink_api_secret" {
  value     = confluent_api_key.flink.secret
  sensitive = true
}

output "confluent_cloud_api_key" {
  value     = var.confluent_cloud_api_key
  sensitive = true
}

output "confluent_cloud_api_secret" {
  value     = var.confluent_cloud_api_secret
  sensitive = true
}

output "deploy_flink_pipeline" {
  value       = var.deploy_flink_pipeline
  description = "false = workshop mode (participants create the AI pipeline statements by hand)"
}

# Read by `uv run setup-rtce`.

output "cloud_provider" {
  value = lower(local.cloud_provider)
}

output "cloud_region" {
  value = local.aws_region
}

output "confluent_rtce_service_account_id" {
  value       = confluent_service_account.rtce_reader.id
  description = "setup_rtce.py creates a Global API key for this service account via the Confluent CLI"
}

output "source_topics" {
  value       = ["transactions", "user_logins", "account_changes"]
  description = "Topics the producer writes; setup-rtce enables RTCE on these"
}

output "served_topics" {
  # activity_profiles is created by hand in workshop mode.
  value       = var.deploy_flink_pipeline ? ["activity_profiles", "fraud_alerts"] : ["fraud_alerts"]
  description = "Topics Flink writes; setup-rtce enables RTCE on these"
}
