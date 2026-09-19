# Ingestion Layer

Everything between *a source emitting data* and *raw tables landing in the warehouse*: live nodes, sidecars, socket listeners and ingestion Flink jobs. Output of this layer is Parquet under Hive layout, managed as [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables.

```
hyperdata-node          hyperdata-indexer-hyperevm      socket-listeners (future)
   │   │                        │                              │
   │   └── snapshots            │                              │
   ▼                           ▼                              ▼
file outputs on shared storage    events
   │                           │
   ▼                           ▼
hyperliquid-node-sidecar ──► Kafka (hyperliquid.node-files, notifications only)
   │
   ▼
flink-jobs/parse-node-outputs ──► Parquet + Iceberg (hypercore.* raw tables)
```

---

## HyperCore pipeline design (deltas + full state snapshots)

HyperCore (and LOB / exchange chains in general) emits **stateful financial data**: per-event deltas (raw book diffs, order statuses, fills) plus periodic **full-state snapshots** (`periodic_abci_states/<date>/<height>.rmp` every ~10k blocks). Critically, HyperLiquid snapshots contain the **entire blockchain state — order books, account balances, margin positions, vaults — not just book state**. This duality shapes the whole pipeline.

```
┌─────────────────── RUNTIME (identical in live / replay / backfill) ───────────────────┐
│                                                                                       │
│  hyperdata-node (hl-visor) — raw JSONL outputs                                         │
│   ├─ live:   p2p stream writes hourly/<date>/<hour> files                             │
│   ├─ replay: script copies recorded outputs into the watched dir on a wall-clock      │
│   │         schedule — downstream cannot distinguish it from a live node               │
│   └─ node also persists periodic_abci_states snapshots (~10k blocks, ~17 min)         │
│                                                                                       │
│  hyperliquid-node-sidecar (watcher service)                                           │
│   └─ watches the node output tree ──► Kafka topic hyperliquid.node-files              │
│      payload = {table, path, date, hour, block_range, size, checksum}                 │
│      (notifications only — the data stays in files, never in Kafka)                    │
│                                                                                       │
│  flink-job: parse-node-outputs (one job, unified bounded/unbounded)                    │
│   ├─ --source kafka   live + replay: consume notifications, tail files                  │
│   ├─ --source files   backfill: bounded enumeration of a file list (S3/SSD)            │
│   └─ core: JSONL → typed rows → group/buffer → Parquet → Iceberg commit                │
│                                                                                       │
│  storage: Parquet, Hive layout (date/hour) + Apache Iceberg tables                     │
│   ├─ local dev: shared SSD volume       ├─ prod: S3 / GCS                              │
│   └─ catalog: REST catalog (Nessie / Polaris class) — one catalog for all engines     │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

**Why this shape:**

- **Replay = live, exactly.** The replay script never invokes the node — historical outputs are copied into the output directory as a fake tap. Downstream (sidecar → Kafka → Flink) cannot distinguish replay from production. One code path tests the entire pipeline.
- **Backfill = same job, different source.** Flink's bounded mode (`--source files`) runs the identical parse core. No bespoke distributed-parser service — it is all one Flink job.
- **Kafka carries metadata, not payloads.** At TB-scale, backfill files are enumerated (e.g. `s5cmd`) and handed to the bounded job directly. Kafka is the live notification bus; it is never a data bus.
- **Iceberg, not raw Parquet piles.** Checkpoint-aligned commits give atomicity, exactly-once semantics, schema evolution and hidden partitioning. Tables: `hypercore.{fills, order_statuses, raw_book_diffs, snapshots}`.

**Infra placement rule:** infra is defined by the first layer that needs it, and promoted to `platform/` when a second layer consumes it. Kafka therefore lives in this layer's `compose.yaml` for now; when transformation/serving start consuming its topics directly, its definition moves to the platform stack unchanged.

**Commit strategy — the one duality of the system:**

| Path | Trigger | Alignment | Optimization |
| :--- | :--- | :--- | :--- |
| Live / replay (unbounded) | Flink checkpoint interval (2–5 min) or writer size (128–512 MB) | time / size | latency-optimal |
| Backfill (bounded) | one task per snapshot-aligned segment `[S_i, S_i+1)` | snapshot boundaries (~10k blocks) | parallelism-optimal |

**Snapshot-aligned backfill — embarrassingly parallel by construction:**

```
discover_snapshots:  list periodic_abci_states  →  segments [S_i, S_i+1)
   └─ map (Airflow dynamic task mapping → Spark/Flink tasks, no shuffle):
        parse_segment[i]:
          load S_i snapshot (translate-abci-state / compute-l4-snapshots)
          → apply deltas deterministically → emit rows
          → validate against trailing snapshot S_i+1   ← free correctness oracle
          → commit 1 parquet file per (table, segment)  ← map-only, no reduce
```

Deltas are not self-contained (an order resting for hours spans many files), but snapshot intervals are: each segment's start-state is fully defined by its snapshot — including **balances and positions**, because HL snapshots are full-state. Segments are disjoint ⇒ zero cross-task communication ⇒ maximally parallel, and each task is self-validating: the replayed end-state must hash-match the real trailing snapshot, otherwise the segment fails and is not written. Segment size (~17 min of chain time) coincidentally lands each file in the 128–512 MB sweet spot, so backfilled data needs no compaction. The same segmentation also yields, for free, point-in-time account/portfolio history: snapshot balances + intra-segment deltas → **historical balance & position reconstruction without traces or archive nodes**.

**Live stateful pre-modeling (Flink):**

- Ingest `raw_book_diffs` keyed by (coin, oid) → reconstruct the full **L4 book stream** in real time, validated against `compute-l4-snapshots` baselines at checkpoint boundaries.
- The same keyed state applies balance/position deltas incrementally; full-state snapshots act as drift correction — periodic re-baselining of Flink state against ground truth.
- Watermarked by block time; reorg-sensitive consumers apply a configurable confirmation buffer (default: 64 blocks).

**Component placement:**

| Component | Home | Engine |
| :--- | :--- | :--- |
| Replay script | `ingestion/hyperdata-node/scripts/` | bash / Python, file copy |
| Sidecar (watcher → Kafka) | `ingestion/hyperliquid-node-sidecar` | Python |
| Parse → Parquet → Iceberg job | `ingestion/flink-jobs/parse-node-outputs` | Flink (PyFlink / Java) |
| Iceberg maintenance (compaction, snapshot expiry) | `../transformation/spark` | Spark / Trino, Airflow-scheduled |

**Milestones (v0.0.1):**

- [ ] Sidecar: file watcher → Kafka topic `hyperliquid.node-files` (Kafka in KRaft mode via docker-compose).
- [ ] Flink job `parse-node-outputs`, bounded mode over a fixture directory (replay-as-backfill PoC).
- [ ] Replay script: schedule copies of recorded hourly outputs into the watched directory.
- [ ] Iceberg REST catalog + `hypercore.*` tables in compose; Parquet writes with schema evolution.
- [ ] Live unbounded mode: Kafka source → checkpoint-committed Iceberg writes, adaptive file sizing.
- [ ] Snapshot-aligned backfill PoC with trailing-snapshot validation (one historical day).
- [ ] Stateful pre-modeling PoC: L4 book reconstruction + balance/position delta apply in Flink state.

---

## Other ingestion services

| Service | What it does |
| :--- | :--- |
| [`hyperdata-node`](hyperdata-node/) | HyperLiquid node (hl-visor) emitting raw output files + periodic full-state snapshots; replay script (planned). |
| [`hyperdata-indexer-hyperevm`](hyperdata-indexer-hyperevm/) | Envio HyperIndex event firehose for HyperEVM (chain 999) → Postgres; raw RPC/HyperSync client per its [spec](hyperdata-indexer-hyperevm/docs/full_indexer_spec.md). |
| `flink-jobs/` | Ingestion-stage Flink jobs (`parse-node-outputs`: JSONL → Parquet → Iceberg). |
| `hyperliquid-node-sidecar` (planned) | File watcher → Kafka notifications. |
| `socket-listeners` (future) | WebSocket feeds from other CEX/DEX venues. |