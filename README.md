# About HyperData Platform

![Version](https://img.shields.io/badge/version-0.0.1-blue) ![Build](https://img.shields.io/badge/build-failing-red) ![License](https://img.shields.io/badge/license-open--source-orange) ![Python](https://img.shields.io/badge/python-3.11%2B-blue) ![Docker](https://img.shields.io/badge/docker-2496ED?logo=docker&logoColor=white) ![Kubernetes](https://img.shields.io/badge/Helm%20%2B%20K8S-326CE5?logo=kubernetes&logoColor=white) ![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white) ![Spark](https://img.shields.io/badge/Spark-E25A1C?logo=apachespark&logoColor=white) ![Flink](https://img.shields.io/badge/Flink-E6526F?logo=apacheflink&logoColor=white) ![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white) ![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)

![Banner](assets/img/banner.png)

What is HyperData Platform?

[todo]

**HyperData** is the open source **[Glassnode](https://docs.glassnode.com/) / [Dune](https://docs.dune.com/) / [CryptoQuant](https://cryptoquant.com/docs) / [Flipside](https://docs.flipsidecrypto.com/) / [Nansen](https://docs.nansen.ai/)** data platform! 

We want Hyperdata to be an open source boilerplate data platform for financial data with core support for any event based financial data system (like CLOB / order book / CEX exchanges) and EVM (or any other VM) based blockchains.

## Why HyperLiquid? 
Well, HyperLiquid's dual chain architecture is the perfect example to build a fully capable data platform for financial time series and blockchain virtual machines:
- HyperCore poses a challenge in terms of real data volume (billions of events) and potential stateful processing (only L4 address book, so allows us to track per address, per asset state).
- HyperEVM is the perfect example of a classic EVM where we can process typical raw EVM data, parse smart contract calls, build data models on top etc. Similarly to how Dune / Glassnode build transformations, abstractions and metrics on top. For this we will approach EVM in an ELT manner, smart contract calls etc will be seen as bronze transformations.

## Status [WIP]

This is an initial draft. We are currently working on the ingestion pipelines **[docs/ingestion.md](docs/ingestion.md)**, with ETA 1-2 weeks to support full ingestion of HyperCore into a central Iceberg catalog. Next steps will be adding EVM near real time ingestion, orchestration and backfills both for HyperCore and HyperEVM, and basic **dbt** modeling to start with.

Then we will start building IaC and kubernetes deployments.

---

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

- **Per-slice compose files** (`platform/kafka/compose.yaml` cluster systems, `services/compose.yaml` services slice), aggregated by the root `docker-compose.yml` via `include:`. Each slice is also runnable standalone: `cd services && docker compose up` brings up just that slice.
- **Top-level axes:** `services/` (long-running deployables) · `jobs/` (compute workloads by engine — placement rule in [`jobs/README.md`](jobs/README.md)) · `platform/` (cluster systems: kafka, flink-cluster, airflow, prefect) · `docs/` (cross-dir execution plans).
- **Project identity:** all compose files set `name: hyperdata-platform`, so included and standalone runs share one project/network identity.
- **No `version:` key** in compose files (obsolete in Compose ≥ 2.20).
- **Profiles gate workloads** that need external inputs (e.g. `--profile node` for the live node + sidecar, `--profile warehouse` for catalog + MinIO); shared systems (Kafka, `platform/kafka/compose.yaml`) are always-on and included by the stack that uses them.

Usage (from repo root):

```bash
docker compose up                                          # platform + shared infra (kafka included)
docker compose --profile node up                           # + hyperdata-node & sidecar
docker compose --profile node --profile warehouse up       # + Iceberg catalog & MinIO
cd services && docker compose up                 # standalone: just the services slice
```

## Ingestion layer: distributed (batch) ingestion & stateful streaming

Milestones:

- [ ] Ingestion of **HyperCore** (order diffs, trades, etc.) node outputs.
- [ ] HyperCore node sidecar service in charge of backups and issuing kafka messages for all different tables (topics). Tailing polling approach (near real time is good enough).

### Hypercore ingestion (or any other Limit Order Book or event feeds)

The full ingestion design — live node, replay-as-tap (progressive append), sidecar line-tailing to per-table Kafka topics, unified Flink bounded/unbounded parsing, snapshot-aligned backfill, Parquet/Iceberg layout — lives in **[docs/ingestion.md](docs/ingestion.md)**. Milestones for v0.0.1 are tracked there.

### HyperEVM ingestion (or any other EVM blockchain)

HyperEVM exposes standard Ethereum JSON-RPC, so raw ingestion reuses **[ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)** — the generic EVM ETL toolchain behind BigQuery's `crypto_ethereum` dataset — pointed at a local HyperLiquid node (`--serve-eth-rpc`) or any RPC provider. One toolchain covers both temporal modes with identical output: **backfill** (batch block-range extraction) and **real-time** (head-following streaming), both materializing the same raw Parquet/Iceberg tables (`blocks`, `transactions`, `logs`, `traces`, `token_transfers`, ...). The [Envio indexer](services/hyperdata-indexer-hyperevm) complements this as the decoded-event firehose (wildcard `Transfer`/`Approval` → Postgres), while ethereum-etl owns the generic raw EVM tables.

Summary of the layer:

| Subsystem | Role |
| :--- | :--- |
| `services/hyperdata-node` | HyperLiquid node (hl-visor) emitting raw output files + full-state snapshots; snapshot bootstrap tooling. |
| `services/hyperdata-node-sidecar` (planned) | Line tailer: streams appended output lines to per-table Kafka topics + hour-file seals; backup sink. |
| `jobs/flink` | `parse-node-outputs`: one Flink job for live, replay and backfill (JSONL → Parquet → Iceberg); cluster in `platform/flink-cluster`. |
| `services/hyperdata-indexer-hyperevm` | HyperEVM event firehose (Envio → Postgres). |
| `services/socket-listeners` (future) | WebSocket feeds for other CEX/DEX venues. |

For HyperEVM raw table extraction (`blocks`, `transactions`, `logs`, `traces`, `contracts`, `tokens`, `token_transfers`, `balances`), see the [indexer spec](services/hyperdata-indexer-hyperevm/docs/full_indexer_spec.md).

## Data warehousing: connectors & supported DBs

TODO:
[ ] Apache Iceberg & Parquet design
[ ] Local testing / development 
[ ] Supported dbs

## Orchestration, data modeling and transformation layer

Modeling, Iceberg maintenance and custom distributed jobs live in **[docs/transformation.md](docs/transformation.md)** (dbt, Spark). Milestones for v0.0.1 are tracked there. Orchestration itself (schedules, dependencies, backfills of tasks) is owned by `platform/airflow`.

### Machine learning, MLOps and custom distributed jobs

## Data serving & analytics layer

## Monitoring & Alerting

## IaC (Infrastructure) & CI/CD
