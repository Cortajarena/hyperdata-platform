# Local Kubernetes (KinD)

Minimal local dev cluster for deploying platform systems on Kubernetes —
the K8s-side counterpart of `docker-compose.yml`. No parity ambition:
compose slices stay the fastest dev loop; this cluster is where systems move
to Helm charts first, matching the roadmap ("IaC & Kubernetes deployments").

## Files

| File | Role |
|---|---|
| `kind-config.yaml` | minimal single-node cluster (commented block shows the first extension point: ingress port mappings) |
| `up.sh` | idempotent cluster create (`kind create cluster`) from the config |

## Prerequisites

- docker
- kind ≥ 0.23 ([install docs](https://kind.sigs.k8s.io/docs/user/quick-start/)) — first-time installs land in `~/.local/bin`; ensure that is on `PATH`
- kubectl (nearby minor to the cluster; compose-free usage from any shell needs the same `PATH` note)

## Usage

```bash
./infrastructure/kind/up.sh          # or: make kind-up
kind delete cluster --name hyperdata  # or: make kind-down
kubectl get nodes
```

## Roadmap (in expected order)

1. Render `platform/kafka/topics.yaml` to Strimzi `KafkaTopic` CRs, or run
   `init-topics.sh` as a one-shot Job (per the K8s note in `docs/ingestion.md`).
2. Deploy Kafka (Strimzi, KRaft) + the Iceberg REST catalog into the cluster.
3. Migrate the `services` slice workloads (node placeholder, sidecar) to
   manifests — compose remains the fast dev loop; K8s is the deployment target.
