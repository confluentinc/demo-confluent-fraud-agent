# ---------------------------------------------------------------------------
# Real-Time Context Engine (RTCE) — read-only service account for the RTCE MCP
# server. Not used by the demo pipeline; it's here so `uv run setup-rtce` works
# out of the box. setup_rtce.py reads confluent_rtce_service_account_id and
# creates the Global-scoped API key the RTCE MCP endpoint requires via the
# Confluent CLI (the Terraform provider can't mint Global keys).
# ---------------------------------------------------------------------------

resource "confluent_service_account" "rtce_reader" {
  display_name = "${local.prefix}-rtce-reader-${random_id.suffix.hex}"
  description  = "Service account for the RTCE MCP server (DeveloperRead on topics + Schema Registry subjects)"
}

resource "confluent_role_binding" "rtce_reader_topics" {
  principal = "User:${confluent_service_account.rtce_reader.id}"
  role_name = "DeveloperRead"
  # DeveloperRead must target a resource type (topic=*), not the cluster root CRN.
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/topic=*"
}

resource "confluent_role_binding" "rtce_reader_subjects" {
  principal = "User:${confluent_service_account.rtce_reader.id}"
  role_name = "DeveloperRead"
  # DeveloperRead on subject=* is the Schema Registry read equivalent.
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=*"
}
