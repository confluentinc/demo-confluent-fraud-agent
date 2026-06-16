resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # --- Fixed configuration (not user-facing) ------------------------------
  cloud_provider = "AWS"
  aws_region     = "us-east-1"
  prefix         = "fraud-agent"

  # Bedrock Claude model. The model id is encoded in the connection endpoint
  # URL (Confluent Cloud Bedrock connections work this way). Change this single
  # line if your IAM user's account lacks access to this model / inference profile.
  bedrock_model_id = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
  bedrock_endpoint = "https://bedrock-runtime.${local.aws_region}.amazonaws.com/model/${local.bedrock_model_id}/invoke"

  # Catalog/database that Flink statements run against.
  catalog  = confluent_environment.main.display_name
  database = confluent_kafka_cluster.standard.display_name
}

resource "confluent_environment" "main" {
  display_name = "${local.prefix}-env-${random_id.suffix.hex}"

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "standard" {
  display_name = "${local.prefix}-cluster-${random_id.suffix.hex}"
  availability = "SINGLE_ZONE"
  cloud        = local.cloud_provider
  region       = local.aws_region
  standard {}
  environment {
    id = confluent_environment.main.id
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.main.id
  }
  depends_on = [confluent_kafka_cluster.standard]
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "main" {
  cloud  = local.cloud_provider
  region = local.aws_region
}

# ---------------------------------------------------------------------------
# Service account + API keys. A single "app-manager" account (EnvironmentAdmin)
# owns the Kafka, Schema Registry, and Flink keys used by Terraform and the
# local producer/dashboard.
# ---------------------------------------------------------------------------

resource "confluent_service_account" "app_manager" {
  display_name = "${local.prefix}-sa-${random_id.suffix.hex}"
  description  = "Manages the fraud-detection demo environment"
}

resource "confluent_role_binding" "app_manager_env_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_api_key" "kafka" {
  display_name = "${local.prefix}-kafka-key-${random_id.suffix.hex}"
  description  = "Kafka API key for producer/dashboard"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind
    environment {
      id = confluent_environment.main.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_api_key" "schema_registry" {
  display_name = "${local.prefix}-sr-key-${random_id.suffix.hex}"
  description  = "Schema Registry API key for producer/dashboard"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.main.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_api_key" "flink" {
  display_name = "${local.prefix}-flink-key-${random_id.suffix.hex}"
  description  = "Flink API key for statement submission"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind
    environment {
      id = confluent_environment.main.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_flink_compute_pool" "main" {
  display_name = "${local.prefix}-pool-${random_id.suffix.hex}"
  cloud        = local.cloud_provider
  region       = local.aws_region
  max_cfu      = 10
  environment {
    id = confluent_environment.main.id
  }
}

# ---------------------------------------------------------------------------
# Data-plane ACLs so the Kafka API key can produce/consume on all topics
# (the producer writes the 3 input topics; the dashboard reads all 4).
# ---------------------------------------------------------------------------

locals {
  acl_topic_ops = {
    write    = "WRITE"
    read     = "READ"
    create   = "CREATE"
    describe = "DESCRIBE"
  }
}

resource "confluent_kafka_acl" "topic" {
  for_each = local.acl_topic_ops
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app_manager.id}"
  host          = "*"
  operation     = each.value
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_acl" "group_read" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app_manager.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}
