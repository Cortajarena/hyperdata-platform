# Components

Per-component documentation lives in a `README.md` next to the code it describes — inside the
component's own repository, versioned with it. That README is the source of truth; this page is
only an index. Cross-cutting design (how components fit together, why) lives in
[Ingestion](ingestion.md) and [Transformation](transformation.md).

Links below point at GitHub so they resolve both here and from the repo. Local equivalents are the
same paths, relative to the repo root.

## Services

Deployable, long-running components (`services/`, [`compose.yaml`](https://github.com/Cortajarena/hyperdata-platform/blob/main/services/compose.yaml)).

| Component | What it does | Docs |
| :--- | :--- | :--- |
| `hyperdata-node` | HyperLiquid node (hl-visor) emitting raw output files + full-state snapshots; snapshot bootstrap tooling | [README](https://github.com/Cortajarena/hyperdata-node/blob/main/README.md) |
| `hyperdata-node-sidecar` | Line tailer: streams appended output lines to per-table Kafka topics + hour-file seals; backup sink | [README](https://github.com/Cortajarena/hyperdata-node-sidecar/blob/main/README.md) |
| `hyperdata-indexer-hyperevm` | HyperEVM (chain 999) event firehose — Envio HyperIndex → Postgres (decoded events) | [README](https://github.com/Cortajarena/hyperdata-indexer-hyperevm/blob/main/README.md) · [spec](https://github.com/Cortajarena/hyperdata-indexer-hyperevm/blob/main/docs/full_indexer_spec.md) |
| `hyperdata-indexer-hypercore` | HyperCore node outputs via batch polling jobs | [README](https://github.com/Cortajarena/hyperdata-indexer-hypercore/blob/main/README.md) |

Slice-level notes (standalone vs included compose, the node upgrade/cutover playbook):
[`services/README.md`](https://github.com/Cortajarena/hyperdata-platform/blob/main/services/README.md).

## Jobs

Compute workloads by engine (`jobs/`).

| Component | Engine | What it does | Docs |
| :--- | :--- | :--- | :--- |
| `jobs/flink` | Flink (Java) | `parse-node-outputs`: node output lines → Parquet → Iceberg; bounded + unbounded | [README](https://github.com/Cortajarena/hyperdata-ingestion-flink/blob/main/README.md) |
| `jobs/dbt` | dbt | SQL models over the warehouse | pending |
| `jobs/spark` | Spark | Iceberg maintenance (compaction, snapshot expiry) | pending |

Layer conventions: [`jobs/README.md`](https://github.com/Cortajarena/hyperdata-platform/blob/main/jobs/README.md).

## Platform

Cluster systems — defined once under `platform/`, included by the stacks that consume them
([`platform/README.md`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/README.md)).

| Component | What it does | Docs |
| :--- | :--- | :--- |
| `platform/kafka` | Kafka (KRaft) + declarative topic contract | pending (see [`topics.yaml`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/kafka/topics.yaml)) |
| `platform/flink-cluster` | Generic session cluster for the Flink jobs | pending |
| `platform/airflow` | Orchestration (DAGs) | pending |
| `platform/clickhouse` | Serving store for derived data | [README](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/clickhouse/README.md) |
| `platform/prefect` | (empty scaffold) | — |

## Infrastructure

Where things run: [`infrastructure/`](https://github.com/Cortajarena/hyperdata-platform/blob/main/infrastructure/README.md)
— local KinD cluster ([`kind/`](https://github.com/Cortajarena/hyperdata-platform/blob/main/infrastructure/kind/README.md)),
GCP terraform ([`gcp/`](https://github.com/Cortajarena/hyperdata-platform/blob/main/infrastructure/gcp/README.md)).

## Shared

Cross-cutting Python package and shared schemas (`shared/`) — not yet populated.

## Repo map

Top-level orientation: [root README](https://github.com/Cortajarena/hyperdata-platform/blob/main/README.md).
