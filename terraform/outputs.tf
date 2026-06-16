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
