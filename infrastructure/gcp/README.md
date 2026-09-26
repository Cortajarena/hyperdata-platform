# GCP / Terraform (infrastructure/gcp)

Minimal infra-as-code for the platform's GCP footprint. Current pieces:
- GCS object storage bucket (warehouse/object store)
- GKE **Autopilot** cluster (Google-managed nodes)

## One-time bootstrap (manual, unavoidable)

```bash
gcloud auth application-default login
gcloud services enable storage.googleapis.com serviceusage.googleapis.com \
    cloudresourcemanager.googleapis.com iam.googleapis.com container.googleapis.com
gcloud storage buckets create gs://<state_bucket> \
    --location=<region> --uniform-bucket-level-access --public-access-prevention
```

## Regular loop

```bash
cp infrastructure/gcp/gcp.tfvars.example infrastructure/gcp/gcp.tfvars   # fill values
make tf-init   # first run only
make tf-plan
make tf-apply
```

After apply: `gcloud container clusters get-credentials <cluster> --region=<region>`.

## Minimum config summary

Cluster is GKE **Autopilot**: `enable_autopilot = true` + `deletion_protection = false`
while dev. Uses project default network with explicit IPv4 ranges. Adding spot
nodes, autoscaling, or labels later = a `google_container_node_pool` block (see
main.tf comments) — Autopilot mode itself supports no inline node pools.
