# Transformation layer

> Map, not a directory: this layer now lives as compute code in `jobs/dbt` and
> `jobs/spark` (see [`jobs/README.md`](../jobs/README.md) for the placement
> rule). This doc keeps the layer's lansdcape — data contracts and ownership —
> in one place.

Everything downstream of raw Iceberg tables: SQL modeling, Iceberg maintenance, and custom distributed jobs.

```
ingestion (services/* + jobs/flink)  ──►  hypercore.* / hyperevm.* raw Iceberg tables
                                   │
                                   ▼
        ┌─────────────────────────────────────────────┐
        │  jobs/dbt        models, tests, semantics   │
        │  jobs/spark      graph jobs, maintenance    │
        │  (future)        serving / ML               │
        └─────────────────────────────────────────────┘
```

| Compute | Role |
| :--- | :--- |
| [`jobs/dbt`](jobs/dbt/) | SQL transformation & tests over the warehouse: staging → intermediate → marts; data-quality tests, freshness checks. |
| [`jobs/spark`](jobs/spark/) | Custom distributed jobs: Iceberg maintenance (compaction, snapshot expiry — Airflow-scheduled), wallet-graph algorithms, anything beyond SQL. |

**Division of labor:**

- **Ingestion writes raw tables, transformation refines them.** Parsers in `services/` (`jobs/flink`) are deliberately dumb: JSONL → typed rows → Parquet. All decoding/business logic (token transfers from raw logs, book analytics, metrics) lives here as dbt models.
- **Iceberg maintenance belongs to Spark here**, not to the ingestion Flink jobs — `rewrite_data_files` / `expire_snapshots` run as scheduled batch actions ([Iceberg maintenance procedures](https://iceberg.apache.org/docs/latest/maintenance/)).
- **Orchestration** (dependencies, schedules) is owned by `platform/airflow`, not by the services themselves.

**Milestones (v0.1.0):**

- [ ] dbt wired to the Iceberg catalog (via Spark/Trino session): staging models over `hypercore.*` raw tables.
- [ ] Data-quality tests: not-null / unique / accepted-values on core marts.
- [ ] Spark job skeleton for Iceberg compaction, callable from Airflow.