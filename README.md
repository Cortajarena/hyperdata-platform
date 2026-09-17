# About HyperData Platform

![Version](https://img.shields.io/badge/version-0.0.1-blue) ![Build](https://img.shields.io/badge/build-failing-red) ![License](https://img.shields.io/badge/license-open--source-orange)

![Python](https://img.shields.io/badge/python-3.11%2B-blue) ![Docker](https://img.shields.io/badge/docker-2496ED?logo=docker&logoColor=white) ![Kubernetes](https://img.shields.io/badge/Helm%20%2B%20K8S-326CE5?logo=kubernetes&logoColor=white)

![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white) ![Spark](https://img.shields.io/badge/Spark-E25A1C?logo=apachespark&logoColor=white) ![Flink](https://img.shields.io/badge/Flink-E6526F?logo=apacheflink&logoColor=white) ![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white) ![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)

![Banner](assets/img/banner.png)

**HyperData** is the open source **[Glassnode](https://docs.glassnode.com/) / [Dune](https://docs.dune.com/) / [CryptoQuant](https://cryptoquant.com/docs) / [Flipside](https://docs.flipsidecrypto.com/) / [Nansen](https://docs.nansen.ai/)** data platform! This README has been proudly written by a human too.

**HyperData Platform** is an **open-source** project born with the following premises:

- **Open source, minimalist, easy to deploy** data platform to process financial data from the **[HyperLiquid](https://hyperliquid.gitbook.io/hyperliquid-docs/) Exchange**.
- With added support for other external financial time-series & **EVM** blockchains, HyperData aims to support (promptly) any **CEX**, **DEX**, **EVM** or similar (like structured financial time series).
- Easy and minimal local development and replication.
- Easy and minimal deployment to any cloud provider (mainly **[AWS](https://docs.aws.amazon.com/)** / **[GCP](https://cloud.google.com/docs)** in the beginning) with **[Helm](https://helm.sh/docs/)** and **[Kubernetes](https://kubernetes.io/docs/home/)**.
- Built-in centralized monitoring & alerting with **[Grafana](https://grafana.com/docs/)** / **[Prometheus](https://prometheus.io/docs/)** as part of the stack.
- Efficient and distributed backfills and disaster recovery tools. Support to plug historical data from providers like **[Dwellir](https://docs.dwellir.com/)** or **[QuickNode](https://www.quicknode.com/docs)**.
- Built-in minimal UI (simple WebApp + **[Apache Superset](https://superset.apache.org/docs/)**) for data exploration and analysis.
- Support streaming data ingestion, complex stateful streaming transformation (pre-modeling) and egestion (**[Kafka](https://kafka.apache.org/documentation/)** + **[Flink](https://nightlies.apache.org/flink/flink-docs-stable/)** / **[Spark](https://spark.apache.org/docs/latest/)** / **[Beam](https://beam.apache.org/documentation/)**) for custom behavior.
- Offline (scheduled) and online (real-time) machine learning and **[MLOps](https://ml-ops.org/)** support.
- Data transformation and orchestration layer with **[Apache Airflow](https://airflow.apache.org/docs/)** & **[dbt](https://docs.getdbt.com/)**, including support for other abstract distributed tasks (for instance, running graph algorithms over wallets with Spark).
- Data quality, freshness, etc. monitoring.
- Data lineage and governance built in.

In the future, we may (or may not) add:

- Trading tools and charts (non real time, intended for long-term analysis like Glassnode or Dune).
- Web3 functionality.
- Better AI-generated slop UI.

You can check the milestones in each section for version **v0.0.1**.

## Fast local development & env simulation [pending]

## Ingestion layer: distributed (batch) ingestion & streaming

Milestones:

- [ ] Ingestion of **HyperCore** (order diffs, trades, etc.) node outputs.

For now, we will be supporting full ingestion of HyperCore's outputs. Later versions will support lightweight and fast ingestion of particular events in **HyperEVM** or the full raw EVM schema:

| Table | Source RPC Method / Strategy for HyperEVM | Complexity |
| :--- | :--- | :--- |
| **`blocks`** | [`eth_getBlockByNumber`](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getblockbynumber) | Low |
| **`transactions`** | Extracted from `eth_getBlockByNumber` | Low |
| **`logs`** | [`eth_getLogs`](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getlogs) or extracted from transaction receipts | Low |
| **`traces`** | [`debug_traceBlockByNumber`](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debugtraceblockbynumber) or `trace_block` | High (Requires Archive Node with tracing enabled) |
| **`contracts`** | Filter traces for `CREATE` operations, then call `eth_getCode` | Medium |
| **`tokens`** | Call standard **[ERC-20](https://docs.openzeppelin.com/contracts/erc20)** / **[ERC-721](https://docs.openzeppelin.com/contracts/erc721)** view functions (`name()`, `symbol()`) on discovered contracts | Medium |
| **`token_transfers`** | Filter logs for the standard `Transfer` event signature | Low |
| **`balances`** | Reconstruct using traces and transactions or sample via `eth_getBalance` | High |

## Orchestration, data modeling and transformation layer

### Machine learning, MLOps and custom distributed jobs

## Data serving & analytics layer

## Monitoring & Alerting

## IaC (Infrastructure) & CI/CD