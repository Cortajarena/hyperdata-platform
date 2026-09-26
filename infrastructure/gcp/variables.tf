variable "project_id" {
  type        = string
  description = "GCP project hosting the platform."
}

variable "region" {
  type    = string
  default = "europe-west1"
  description = "GCP region. Latency-sensitive: keep near consumers/exchange infra when planning CEX ingestion."
}

variable "state_bucket" {
  type        = string
  description = "GCS bucket holding terraform state (also passed to backend via -backend-config on init)."
}

variable "cluster_name" {
  type    = string
  default = "hyperdata-gke"
}

variable "node_pool_name" {
  type    = string
  default = "default-pool"
}

variable "machine_type" {
  type    = string
  default = "e2-medium"
}

locals {
  workspace_bucket = "hyperdata-warehouse"
}
