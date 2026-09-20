# About HyperData Platform

![Version](https://img.shields.io/badge/version-0.0.1-blue) ![Build](https://img.shields.io/badge/build-failing-red) ![License](https://img.shields.io/badge/license-open--source-orange) ![Python](https://img.shields.io/badge/python-3.11%2B-blue) ![Docker](https://img.shields.io/badge/docker-2496ED?logo=docker&logoColor=white) ![Kubernetes](https://img.shields.io/badge/Helm%20%2B%20K8S-326CE5?logo=kubernetes&logoColor=white) ![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white) ![Spark](https://img.shields.io/badge/Spark-E25A1C?logo=apachespark&logoColor=white) ![Flink](https://img.shields.io/badge/Flink-E6526F?logo=apacheflink&logoColor=white) ![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white) ![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)

![Banner](assets/img/banner.png)

What is HyperData Platform?

[todo]

**HyperData** is the open source **[Glassnode](https://docs.glassnode.com/) / [Dune](https://docs.dune.com/) / [CryptoQuant](https://cryptoquant.com/docs) / [Flipside](https://docs.flipsidecrypto.com/) / [Nansen](https://docs.nansen.ai/)** data platform! We want Hyperdata to be an open source boilerplate data platform for financial data with 

Why HyperLiquid? Well, HyperLiquid's dual chain architecture is the perfect

**HyperData Platform** is an **open-source** project born with the following premises:

- **Open source, minimalist, easy to deploy** data platform to process financial data from the **[HyperLiquid](https://hyperliquid.gitbook.io/hyperliquid-docs/) Exchange** (or any other event based financial data source, exchange or chain).
- With added support for other external financial time-series & **EVM** blockchains, HyperData aims to support (promptly) any **CEX**, **DEX**, **EVM** or similar (like structured financial time series). We will provide examples to build a full data model for EVM blockchains (which could also serve as a starting point for non-EVM chains), from raw data to layer based data and semantic models.
- Easy and minimal local development and replication.
- Easy and minimal deployment to any cloud provider (mainly **[AWS](https://docs.aws.amazon.com/)** / **[GCP](https://cloud.google.com/docs)** in the beginning) with **[Helm](https://helm.sh/docs/)** and **[Kubernetes](https://kubernetes.io/docs/home/)**.
- Support for multiple centralized data warehousing options, like **[BigQuery](https://cloud.google.com/bigquery/docs)**, **[ClickHouse](https://clickhouse.com/docs)**, **[Redshift](https://docs.aws.amazon.com/redshift/)**, etc.
- Built-in centralized monitoring & alerting with **[Grafana](https://grafana.com/docs/)** / **[Prometheus](https://prometheus.io/docs/)** as part of the stack.
- Efficient and distributed backfills and disaster recovery tools. Support to plug historical data (both raw node output archives, parsed **[Parquet](https://parquet.apache.org/docs/)** / **[Apache Iceberg](https://iceberg.apache.org/docs/latest/)** or others) from providers like:
  - **[Dwellir](https://docs.dwellir.com/)**
  - **[QuickNode](https://www.quicknode.com/docs)**
  - **[BitQuery](https://docs.bitquery.io/)**
  - **[The Indexing Company](https://docs.indexing.co/)**
  - **[CryptoStruct](https://docs.cryptostruct.com/)**
- Built-in minimal UI (simple WebApp + **[Apache Superset](https://superset.apache.org/docs/)**) for data exploration and analysis.
- Support streaming data ingestion, complex stateful streaming transformation (pre-modeling) and egestion (**[Kafka](https://kafka.apache.org/documentation/)** + **[Flink](https://nightlies.apache.org/flink/flink-docs-stable/)** / **[Spark](https://spark.apache.org/docs/latest/)** / **[Beam](https://beam.apache.org/documentation/)**) for custom behavior.
- Offline (scheduled) and online (real-time) machine learning and **[MLOps](https://ml-ops.org/)** support.
- Data transformation and orchestration layer with **[Apache Airflow](https://airflow.apache.org/docs/)** & **[dbt](https://docs.getdbt.com/)**, including support for other abstract distributed tasks (for instance, running graph algorithms over wallets with Spark).
- Data quality, freshness, etc. monitoring.
- Data lineage and governance built in.

In the future, we may (or may not) add:

- Trading tools and charts (non real time, intended for long-term analysis like Glassnode or Dune).
- BYOL - bring your own GCP / BigQuery project, requester (you) pays for the query, we maintain the data.
- BYOL - bring your own OpenRouter / Anthropic / OpenAI, etc. key, use agents to explore the data with your GCP / AWS project.
- Web3 functionality.
- Better AI-generated slop UI.

You can check the milestones in each section for version **v0.0.1**.

## Fast local development & env simulation [pending]

Local stack conventions:

- **Per-layer compose files** (`services/ingestion/compose.yaml`, ...), aggregated by the root `docker-compose.yml` via `include:`. Each layer stack is also runnable standalone: `cd services/ingestion && docker compose up` brings up just that slice.
- **Project identity:** all compose files set `name: hyperdata-platform`, so included and standalone runs share one project/network identity.
- **No `version:` key** in compose files (obsolete in Compose ≥ 2.20).
- **Profiles gate workloads** that need external inputs (e.g. `--profile node` for the live node + sidecar, `--profile warehouse` for catalog + MinIO); shared infra (Kafka) is always-on within its layer.

Usage (from repo root):

```bash
docker compose up                                          # platform + shared infra (kafka included)
docker compose --profile node up                           # + hyperdata-node & sidecar
docker compose --profile node --profile warehouse up       # + Iceberg catalog & MinIO
cd services/ingestion && docker compose up                 # standalone: just the ingestion slice
```

## Ingestion layer: distributed (batch) ingestion & stateful streaming

Milestones:

- [ ] Ingestion of **HyperCore** (order diffs, trades, etc.) node outputs.
- [ ] Supercore node sidecar service in charge of backups and issuing kafka messages.

### Hypercore ingestion (or any other Limit Order Book or event feeds)

The full ingestion design — live node, replay-as-tap, sidecar → Kafka notifications, unified Flink bounded/unbounded parsing, snapshot-aligned backfill, Parquet/Iceberg layout — lives in **[services/ingestion/README.md](services/ingestion/README.md)**. Milestones for v0.0.1 are tracked there.

### HyperEVM ingestion (or any other EVM blockchain)

HyperEVM exposes standard Ethereum JSON-RPC, so raw ingestion reuses **[ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)** — the generic EVM ETL toolchain behind BigQuery's `crypto_ethereum` dataset — pointed at a local HyperLiquid node (`--serve-eth-rpc`) or any RPC provider. One toolchain covers both temporal modes with identical output: **backfill** (batch block-range extraction) and **real-time** (head-following streaming), both materializing the same raw Parquet/Iceberg tables (`blocks`, `transactions`, `logs`, `traces`, `token_transfers`, ...). The [Envio indexer](services/ingestion/hyperdata-indexer-hyperevm) complements this as the decoded-event firehose (wildcard `Transfer`/`Approval` → Postgres), while ethereum-etl owns the generic raw EVM tables.

Summary of the layer:

| Subsystem | Role |
| :--- | :--- |
| `services/ingestion/hyperdata-node` | HyperLiquid node (hl-visor) emitting raw output files + full-state snapshots; replay script (planned). |
| `services/ingestion/hyperdata-node-sidecar` (planned) | Watches node outputs, publishes file notifications to Kafka. |
| `services/ingestion/hyperdata-ingestion-flink` | `parse-node-outputs`: one Flink job for live, replay and backfill (JSONL → Parquet → Iceberg). |
| `services/ingestion/hyperdata-indexer-hyperevm` | HyperEVM event firehose (Envio → Postgres). |
| `services/ingestion/socket-listeners` (future) | WebSocket feeds for other CEX/DEX venues. |

For HyperEVM raw table extraction (`blocks`, `transactions`, `logs`, `traces`, `contracts`, `tokens`, `token_transfers`, `balances`), see the [indexer spec](services/ingestion/hyperdata-indexer-hyperevm/docs/full_indexer_spec.md).

## Data warehousing: connectors & supported DBs

TODO:
[ ] Apache Iceberg & Parquet design
[ ] Local testing / development 
[ ] Supported dbs

## Orchestration, data modeling and transformation layer

Modeling, Iceberg maintenance and custom distributed jobs live in **[services/transformation/README.md](services/transformation/README.md)** (dbt, Spark). Milestones for v0.0.1 are tracked there. Orchestration itself (schedules, dependencies, backfills of tasks) is owned by `platform/airflow`.

### Machine learning, MLOps and custom distributed jobs

## Data serving & analytics layer

## Monitoring & Alerting

## IaC (Infrastructure) & CI/CD
