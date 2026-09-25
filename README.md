# About HyperData Platform

| Category | Badges |
|----------|--------|
| Release | ![Version](https://img.shields.io/badge/version-0.1.0-blue) · ![License](https://img.shields.io/badge/license-open--source-orange) |
| Languages | ![Python](https://img.shields.io/badge/python-3.11%2B-green) |
| Infrastructure | ![Docker](https://img.shields.io/badge/compose-%E2%89%A52.20-2496ED?logo=docker&logoColor=white) · ![Kubernetes](https://img.shields.io/badge/K8s-1.37-326CE5?logo=kubernetes&logoColor=white) · ![KinD](https://img.shields.io/badge/KinD-0.33-326CE5) |
| Streaming | ![Kafka](https://img.shields.io/badge/Kafka-3.9.0-231F20?logo=apachekafka&logoColor=white) · ![Flink](https://img.shields.io/badge/Flink-1.18-E6526F?logo=apacheflink&logoColor=white) · ![Trino](https://img.shields.io/badge/Trino-pending-yellow) |
| Modeling & orchestration | ![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white) · ![Spark](https://img.shields.io/badge/Spark-3.5.1-E25A1B?logo=apachespark&logoColor=white) · ![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white) |
| Serving | ![ClickHouse](https://img.shields.io/badge/ClickHouse-26.9-yellow) |

**HyperData** is an open-source **data platform for financial data** — an open-source alternative to the Glassnode / Dune / CryptoQuant / Flipside / Nansen class of products.

We want HyperData to be an open source boilerplate data platform for financial data with core support for any event based financial data system (like CLOB / order book / CEX exchanges) and EVM (or any other VM) based blockchains.

## Why HyperLiquid?
Well, HyperLiquid's dual chain architecture is the perfect example to build a fully capable data platform for financial time series and blockchain virtual machines:

- HyperCore poses a challenge in terms of real data volume (billions of events) and potential stateful processing (only L4 address book, so allows us to track per address, per asset state).
- HyperEVM is the perfect example of a classic EVM where we can process typical raw EVM data, parse smart contract calls, build data models on top etc. Similarly to how Dune / Glassnode build transformations, abstractions and metrics on top. For this we will approach EVM in an ELT manner, smart contract calls etc will be seen as bronze transformations.

## Status [WIP]

This is an initial draft. We are currently working on the ingestion pipelines **[docs/ingestion.md](docs/ingestion.md)**, with ETA 1-2 weeks to support full ingestion of HyperCore into a central Iceberg catalog. Next steps will be adding EVM near real time ingestion, orchestration and backfills both for HyperCore and HyperEVM, and basic **dbt** modeling to start with. Then we will start building IaC and kubernetes deployments.

## Repository layout

| Dir | What lives here |
|---|---|
| [`services/`](services/) | Long-running deployables: `hyperdata-node`, `hyperdata-node-sidecar` (planned), `hyperdata-indexer-hyperevm` |
| [`jobs/`](jobs/) | Compute code by engine: `jobs/flink` (HyperCore parse → Iceberg), `jobs/dbt`, `jobs/spark` |
| [`platform/`](platform/) | Cluster systems: Kafka, ClickHouse (serving store), Flink cluster, Airflow, Prefect |
| [`infrastructure/`](infrastructure/) | Where things run: kind dev cluster, terraform/pending |
| [`docs/`](docs/) | Execution plans and architecture notes ([ingestion](docs/ingestion.md), [transformation](docs/transformation.md)) |

Local stack conventions:

- **Per-slice compose files** (`platform/kafka/compose.yaml`, `services/compose.yaml`), aggregated by the root `docker-compose.yml` via `include:`; slices are standalone-runnable.
- **No `version:` key** in compose files (Compose ≥ 2.20) and a fixed `name: hyperdata-platform` across compose files so included/standalone runs share identity.
- **Profiles gate workloads** needing external inputs (`--profile node` for the node + sidecar, `--profile warehouse` for catalog + storage); cluster systems (Kafka) are always-on.

Usage (from repo root):

```bash
docker compose up                                          # platform + shared systems (Kafka included)
docker compose --profile node up                           # + hyperdata-node & sidecar
docker compose --profile node --profile warehouse up       # + Iceberg catalog & MinIO
cd services && docker compose up                           # standalone: just the services slice
make kind-up                                               # minimal local Kubernetes (KinD) dev cluster
```

## Ingestion layer: distributed (batch) ingestion & stateful streaming

The full ingestion design — live node, replay-as-tap (progressive append), sidecar line-tailing to per-table Kafka topics, unified Flink bounded/unbounded parsing, snapshot-aligned backfill, Parquet/Iceberg layout — lives in **[docs/ingestion.md](docs/ingestion.md)**. Milestones for v0.1.0 are tracked there.

Near-term milestones: ingestion of **HyperCore** node outputs (order diffs, trades, etc.), plus the HyperCore node sidecar issuing Kafka messages for all tables (topics) — tailing/polling approach, near real time is good enough.

### HyperEVM ingestion (or any other EVM blockchain)

HyperEVM exposes standard Ethereum JSON-RPC, so raw ingestion reuses **[ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)** — the generic EVM ETL toolchain behind BigQuery's `crypto_ethereum` dataset — pointed at a local HyperLiquid node (`--serve-eth-rpc`) or any RPC provider. One toolchain covers both temporal modes with identical output: **backfill** (batch block-range extraction) and **real-time** (head-following streaming), both materializing the same raw Parquet/Iceberg tables (`blocks`, `transactions`, `logs`, `traces`, `token_transfers`, ...). The [Envio indexer](services/hyperdata-indexer-hyperevm) complements this as the decoded-event firehose (wildcard `Transfer`/`Approval` → Postgres), while ethereum-etl owns the generic raw EVM tables.

For HyperEVM raw table extraction (`blocks`, `transactions`, `logs`, `traces`, `contracts`, `tokens`, `token_transfers`, `balances`), see the [indexer spec](services/hyperdata-indexer-hyperevm/docs/full_indexer_spec.md).

Summary of the layer:

| Subsystem | Role |
| :--- | :--- |
| [`services/hyperdata-node`](services/hyperdata-node) | HyperLiquid node (hl-visor) emitting raw output files + full-state snapshots; snapshot bootstrap tooling. |
| [`services/hyperdata-node-sidecar`](services/hyperdata-node-sidecar) (planned) | Line tailer: streams appended output lines to per-table Kafka topics + hour-file seals; backup sink. |
| [`jobs/flink`](jobs/flink) | `parse-node-outputs`: one Flink job for live, replay and backfill (JSONL → Parquet → Iceberg); cluster in `platform/flink-cluster`. |
| [`services/hyperdata-indexer-hyperevm`](services/hyperdata-indexer-hyperevm) | HyperEVM event firehose (Envio → Postgres). |
| `services/socket-listeners` (future) | WebSocket feeds for other CEX/DEX venues. |

## Orchestration, data modeling and transformation layer

Modeling, Iceberg maintenance and custom distributed jobs live in **[docs/transformation.md](docs/transformation.md)** (dbt, Spark). Milestones for v0.1.0 are tracked there. Orchestration itself (schedules, dependencies, backfills of tasks) is owned by `platform/airflow`.

## Data serving & analytics layer

ClickHouse is pre-configured as the serving store for the derived data tier
(book-state streams, Phase 5): single definition in
[`platform/clickhouse/`](platform/clickhouse/). Quickstart in that
directory's README.

## IaC (Infrastructure) & CI/CD [planned]

The tooling home is [`infrastructure/`](infrastructure/) — the kind dev
cluster is live; terraform + real Kubernetes manifests come next (see the
[K8s roadmap](infrastructure/kind/README.md#roadmap-in-expected-order)).
