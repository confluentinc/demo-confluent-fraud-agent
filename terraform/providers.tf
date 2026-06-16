terraform {
  required_version = ">= 1.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.38"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

provider "random" {}
provider "local" {}
