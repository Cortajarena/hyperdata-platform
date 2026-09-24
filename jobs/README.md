# Jobs

Compute code that runs as workloads **submitted into a shared runtime** —
bounded or unbounded. This is the placement rule, not uptime: an unbounded
Flink streaming job still lives here, because the long-lived process is owned
by the platform cluster (`platform/flink-cluster`), not by the job code.
Deployable always-on components (own port/files, deployed as containers) go in
[`../services/`](../services/) instead. Engine groups share toolchain
affinity; each job's *data-flow ownership*, where it matters, is recorded in
[docs/](../docs/).

| Engine | Jobs | Runs on |
| :--- | :--- | :--- |
| [`flink/`](flink/) | `parse-node-outputs`: HyperLiquid node output JSONL → typed rows → Parquet → Iceberg (`hypercore.*` raw tables). Dual-mode: `--source files` (bounded backfill) / Kafka (unbounded live/replay). | platform Flink cluster (Java fat-jar) |
| [`dbt/`](dbt/) | SQL modeling + data-quality tests over the warehouse (staging → intermediate → marts). | dbt engine, triggered from Airflow |
| [`spark/`](spark/) | Iceberg maintenance (compaction, snapshot expiry — Airflow-scheduled), wallet-graph algorithms | Spark cluster |

Cluster definitions for Flink live in `platform/flink-cluster`; the Spark
master wiring currently builds from `spark/` (TODO: split cluster from job
code the same way, `platform/spark-cluster`).
