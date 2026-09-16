# About HyperData Platform

Hyperdata is the Glassnode / Dune / CryptoQuant / Flipside / Nansen open source data platform !

HyperData Platform is an open-source project that was born with the following premises:
- Open source, minimalist, easy to deploy data platform to process financial data from the HyperLiquid Exchange.
- With added to support to other external financial time-series & EVM blockchains, HyperData aims to support (promptly) any CEX, DEX, EVM or similar (like structured financial time series).
- Easy and minimal local development and replication.
- Easy and minimal deployment to any cloud provider (mainly AWS / GCP in the begining) with HELM and K8S.
- Built in monitoring with Grafana / Prometheus as part of the stack.
- Efficient and distributed backfills and disaster recovery tools. Support to plug historical data from providers like Dwellir or Quicknode.
- Builtin minimal UI (simple WebApp + Apache Superset) for data exploration and analysis.
- Support streaming data ingestion and egestion (Flink, Spark, Beam) and transformation for custom behavior.
- Offline (scheduled) and online (real-time) machine learning and MLOps support.
- Data fransformation and orchestration layer with Airflow & dbt, including support for other abstract distributed task (for instance, running graph algorithms over wallets with Spark).

In the future, we may (or may not) add:
- Trading tools and charts (non real time, intended for long term analysis like Glassnode or Dune).
- Web3 functionality.
- Better AI generated slop UI.

## Ingestion layer

For now, we will be supporting ful ingestion of HyperCore's outputs. Later versions will support lightweight and fast ingestion of particular events in HyperEVM or the full raw EVM schema:

| BigQuery Table | Source RPC Method / Strategy for HyperEVM | Complexity |
| :--- | :--- | :--- |
| **`blocks`** | `eth_getBlockByNumber` | Low |
| **`transactions`** | Extracted from `eth_getBlockByNumber` | Low |
| **`logs`** | `eth_getLogs` or extracted from transaction receipts | Low |
| **`traces`** | `debug_traceBlockByNumber` or `trace_block` | High (Requires Archive Node with tracing enabled) [1, 2] |
| **`contracts`** | Filter traces for `CREATE` operations, then call `eth_getCode` | Medium |
| **`tokens`** | Call standard ERC-20/ERC-721 view functions (`name()`, `symbol()`) on discovered contracts | Medium |
| **`token_transfers`** | Filter logs for the standard `Transfer` event signature | Low |
| **`balances`** | Reconstruct using traces and transactions or sample via `eth_getBalance` | High |
