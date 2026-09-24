# Infrastructure

Tooling that **creates environments and manages deployment targets** — the
"where things run" counterpart to `platform/` (which defines *what* runs).

| Subdir | Role |
|---|---|
| [`kind/`](kind/) | local Kubernetes dev cluster (KinD): minimal config + idempotent bootstrap script |
| `terraform/` | cloud IaC (staging / prod deploy targets — pending) |
| `docker/` | docker-adjacent tooling beyond the compose stacks (pending) |

Deployable system manifests (Helm charts, CRs — e.g. Strimzi rendering of
`platform/kafka/topics.yaml`) will live under `kubernetes/` here once the
local cluster is green.
