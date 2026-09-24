# Platform

Definitions of **cluster systems** — the "what runs" answer (vs
[`infrastructure/`](../infrastructure/), the "where it runs", and
[`jobs/`](../jobs/), the "what computes into what"). Every subdir owns one
system's runtime contract; consumers include its compose fragment rather
than redeclaring it.

| System | Dir | Status |
|---|---|---|
| Kafka (KRaft) | [`kafka/`](kafka/) | live — topic contract + bootstrap + compose fragment |
| ClickHouse (serving store) | [`clickhouse/`](clickhouse/) | scaffolded, quickstart verified, `serving`-gated |
| Flink runtime | [`flink-cluster/`](flink-cluster/) | generic image — connectors live in job jars |
| Airflow | [`airflow/`](airflow/) | orchestration (schedules, backfills, task graphs) |
| Prefect | [`prefect/`](prefect/) | alternative/secondary orchestration |

Rules of the dir: system dirs carry **runtime config only** — no models, no
table logic, no data (that belongs to `jobs/`, which is written from
S3 sources by an owner *engine*, not by the DB it lands in).

---

## Data architecture: engines are decoupled from the serving DB

The platform separates **two kinds of compute systems**, and the contract
between them is one-way by design:

```
            ┌─ distributed engines (own the data) ──────────────┐
sources ──► │ Trino / Spark / dbt   — query + transform Iceberg │
            │ Flink                 — streaming state + sinks   │
            │ (BigQuery, Redshift, ... BYOL warehouse options)  │
            └───────────────┬───────────────────────────────────┘
                            │ writes Iceberg tables + Kafka topics
                            ▼ (the only legal direction)
            ┌─ serving store (disposable projection) ───────────┐
            │ ClickHouse: serving tables, MVs, BI read replicas │
            └───────────────────────────────────────────────────┘
```

**Contract rules:**

1. **Engines anchor the truth.** Iceberg is the archive (Parquet, hidden
   partitioning, time travel). Every distributed engine reads the same
   catalog; none of them is special. Swapping Spark ↔ Trino ↔ BigQuery is a
   session/config change, not a migration.
2. **The serving DB is a projection, never a source of truth.** ClickHouse
   (or any serving-tier DB) may be wiped and rebuilt from its two upstream
   planes at any time: Kafka replay + mart re-pull. No app logic assumes CH
   was the origin of anything.
3. **Single-writer per metric.** Streaming aggregates have one producer
   (jobs/flink); batch marts one producer (jobs/dbt on a chosen engine). No
   recomputation of the same metric in two engines.
4. **No reverse flow.** Nothing writes back from the serving store into
   Iceberg or the warehouse. Evaluation loops land as jobs writing new
   tables, keeping engines append-authoritative.
5. **Cube-independence yields engine-agnostic serving.** Because the serving
   store only consumes what upstream wrote, ClickHouse can be replaced by
   Redis/other store per dataset without touching the engine layer.
6. **No "stream-after-transformation" system.** There is no extra streaming
   machinery pushing dbt/Spark outputs anywhere, and Spark/dbt never *need*
   to stream: batch marts publish to Iceberg and are loaded to the serving
   tier by the **same DAG's final task** (Airflow/Prefect). Only when a
   dataset must be seconds-fresh does it graduate into a `jobs/flink` job
   writing a derived Kafka topic — an engine-level promotion by freshness
   requirement, never by connector choice.

**Loading modes** (per dataset, declared next to its definition in `jobs/`):

| Data | Transport | Freshness | Sync model |
|---|---|---|---|
| Real-time derived streams | Flink → Kafka topic → CH **Kafka table engine + MV** | seconds | push-subscription (no polling) — see below |
| Batch marts / reference data | **Final step of the publishing DAG/flow**: an Airflow/Prefect task (or `dbt-clickhouse` incremental model) runs the CH-side `INSERT INTO ... SELECT` over the Iceberg source (Iceberg catalog integration or Parquet via S3 table function) | minutes–hours (DAG cadence decides) | push-triggered from the DAG; no continuous poll anywhere |
| Ad-hoc exploration | none — query the lake directly (S3/Parquet or Iceberg via engine of choice) | — | no serving copy at all |

The serving store never polls the architecture that feeds it. When a batch
publish lands, the *orchestration* (Airflow) fires the load; when a stream
emits, Kafka consumer groups slide it in. ClickHouse applications stay dumb
readers.

### Why the serving DB doesn't have to poll

**Streaming side — Kafka table engine + materialized view** (no app-level
polling, no external sync job): CH runs a consumer-group subscription on the
derived topic; the broker streams records to it; a per-topic MV materializes
each micro-batch (`JSONEachRow`, zstd) into a MergeTree/AggregatingMergeTree
target. Backpressure is absorbed by Kafka retention — CH restarts replay
from offsets, nothing is lost. Efficiency knobs that matter:
`kafka_num_consumers` ≈ number of brokers/consumer threads, one MV per
target table (MVs already batch; don't double-buffer with `async_insert`;
tune `kafka_max_block_size` / `kafka_flush_interval`), and never per-row
UPDATEs — order-keyed immutable streams + incremental aggregating state via
`AggregatingMergeTree` / projections, or state events re-emitted by Flink.

**Batch side (marts) — orchestration-triggered, not polled**: CH holds no
pull loop at all. When dbt publishes a partition, Airflow fires a task that
runs `INSERT INTO serving.X SELECT ... FROM iceberg(...)` (or Parquet via
S3 table function) — the load happens exactly once per published partition.
No latency-chasing add-ons, no CH catalog watcher, and the trigger state
lives where job state already lives (Airflow), not in the DB.

**If true zero-hop push is ever needed** (sub-second, no broker buffering):
the only option is the producer inserting directly via CH's HTTP/native
inserter (Flink sink, dbt writes). **Parked — not a designed & self-sustaining
path** — it couples CH availability to producer backpressure and drops the
Kafka backpressure sponge; revisit only if Kafka's millisecond hop is
measured as a production bottleneck, never by default.
