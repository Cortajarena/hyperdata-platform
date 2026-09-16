# Hyperdata Platform

A unified polyglot data platform monorepo combining orchestration, batch
processing, stream processing, and data modeling into a single deployable
stack.

## Stack

| Component        | Technology | Purpose                          |
| ---------------- | ---------- | -------------------------------- |
| Orchestration    | Airflow    | DAG scheduling and monitoring    |
| Batch processing | Spark      | Large-scale batch workloads      |
| Stream processing| Flink      | Real-time event processing       |
| Data modeling    | dbt        | SQL transformations and testing  |
| Metadata         | Postgres   | Airflow and dbt backing database |
| Hyperdata Node   | Submodule  | Live/gossip data node service    |

## Quick start

```bash
make up        # start all services
make logs      # follow logs
```

- Airflow UI: <http://localhost:8080>
- Flink UI: <http://localhost:8081>
- Spark UI: <http://localhost:9090>
- Postgres: `localhost:5432`

See the [Getting Started](getting-started/installation.md) guide for a full
walkthrough.