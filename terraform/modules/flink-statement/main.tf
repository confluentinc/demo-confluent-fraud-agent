terraform {
  required_providers {
    confluent = {
      source = "confluentinc/confluent"
    }
  }
}

variable "organization_id" { type = string }
variable "environment_id" { type = string }
variable "compute_pool_id" { type = string }
variable "principal_id" { type = string }
variable "rest_endpoint" { type = string }
variable "flink_api_key" {
  type      = string
  sensitive = true
}
variable "flink_api_secret" {
  type      = string
  sensitive = true
}
variable "catalog" { type = string }
variable "database" { type = string }
variable "statement_name" { type = string }
variable "statement" { type = string }
variable "extra_properties" {
  type    = map(string)
  default = {}
}

resource "confluent_flink_statement" "this" {
  organization {
    id = var.organization_id
  }
  environment {
    id = var.environment_id
  }
  compute_pool {
    id = var.compute_pool_id
  }
  principal {
    id = var.principal_id
  }
  rest_endpoint = var.rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  statement_name = var.statement_name
  statement      = var.statement

  properties = merge({
    "sql.current-catalog"  = var.catalog
    "sql.current-database" = var.database
  }, var.extra_properties)
}

output "id" {
  value = confluent_flink_statement.this.id
}
