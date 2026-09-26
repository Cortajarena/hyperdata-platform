# Terraform remote state in GCS. The state bucket is created ONCE manually
# (chicken-and-egg: cannot bootstrap its own backend):
#
#   gcloud auth application-default login
#   gcloud services enable storage.googleapis.com serviceusage.googleapis.com \
#       cloudresourcemanager.googleapis.com iam.googleapis.com container.googleapis.com
#   gcloud storage buckets create gs://<var.state_bucket> \
#       --location=<var.region> --uniform-bucket-level-access --public-access-prevention
#
terraform {
  required_version = ">= 1.9.0"

  backend "gcs" {
    prefix = "env/default"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
