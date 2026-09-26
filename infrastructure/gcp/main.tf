# ---- warehouse/object storage ------------------------------------------
resource "google_storage_bucket" "warehouse" {
  name                        = local.workspace_bucket
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true # dev: allow wipe; flip before prod data
}

# ---- GKE ---------------------------------------------------------------
# Minimal cluster. Two deviations from "make it beautiful":
#  * uses the project default network (add a google_compute_network/subnetwork
#    pair + ip_allocation_policy for serious VPC hygiene later)
#  * no node pool is declared explicitly => GKE gives one default pool of
#    e2-medium. When you want spot/preemptible, autoscaling, or labels, turn
#    the commented node pool on instead of inline google_container_node_pool.
resource "google_container_cluster" "main" {
  name     = var.cluster_name
  location = var.region

  # Autopilot = Google-managed nodes; simplest & cheapest correct default.
  # Flip to master_auth / node pool block to go Standard mode later.
  enable_autopilot    = true
  deletion_protection = false # dev posture; set true before prod

  # required for autopilot with default network: let GKE pick ranges itself
  ip_allocation_policy {
    cluster_ipv4_cidr_block  = "10.4.0.0/14"
    services_ipv4_cidr_block = "10.8.0.0/20"
  }
}

# ---- kubectl wiring (optional convenience export) ----------------------
# After `terraform apply`, run:
#   gcloud container clusters get-credentials --region=europe-west1 hyperdata-gke
