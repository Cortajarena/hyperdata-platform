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
hyperdata-node-sidecar ──► Kafka (hyperliquid.node-files, notifications only)
   │
   ▼
hyperdata-ingestion-flink/parse-node-outputs ──► Parquet + Iceberg (hypercore.* raw tables)
```

---

## HyperCore pipeline design (deltas + full state snapshots)

HyperCore (and LOB / exchange chains in general) emits **stateful financial data**: per-event deltas (raw book diffs, order statuses, fills) plus periodic **full-state snapshots** (`periodic_abci_states/<date>/<height>.rmp` every ~10k blocks). Critically, HyperLiquid snapshots contain the **entire blockchain state — order books, account balances, margin positions, vaults — not just book state**. This duality shapes the whole pipeline.

```
┌─────────────────── RUNTIME (identical in live / replay / backfill) ───────────────────┐
│                                                                                       │
│  hyperdata-node (hl-visor) — raw JSONL outputs                                        │
│   ├─ live:   p2p stream writes hourly/<date>/<hour> files                             │
│   ├─ replay: script copies recorded outputs into the watched dir on a wall-clock      │
│   │         schedule — downstream cannot distinguish it from a live node              │
│   └─ node also persists periodic_abci_states snapshots (~10k blocks, ~17 min)         │
│                                                                                       │
│  hyperdata-node-sidecar (watcher service)                                             │
│   └─ watches the node output tree ──► Kafka topic hyperliquid.node-files              │
│      payload = {table, path, date, hour, block_range, size, checksum}                 │
│      (notifications only — the data stays in files, never in Kafka)                   │
│                                                                                       │
│  flink-job: parse-node-outputs (one job, unified bounded/unbounded)                   │
│   ├─ --source kafka   live + replay: consume notifications, tail files                │
│   ├─ --source files   backfill: bounded enumeration of a file list (S3/SSD)           │
│   └─ core: JSONL → typed rows → group/buffer → Parquet → Iceberg commit               │
│                                                                                       │
│  storage: Parquet, Hive layout (date/hour) + Apache Iceberg tables                    │
│   ├─ local dev: shared SSD volume       ├─ prod: S3 / GCS                             │
│   └─ catalog: REST catalog (Nessie / Polaris class) — one catalog for all engines     │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

**Why this shape:**

- **Replay = live, exactly.** The replay script never invokes the node — historical outputs are copied into the output directory as a fake tap. Downstream (sidecar → Kafka → Flink) cannot distinguish replay from production. One code path tests the entire pipeline.
- **Backfill = same job, different source.** Flink's bounded mode (`--source files`) runs the identical parse core. No bespoke distributed-parser service — it is all one Flink job.
- **Kafka carries metadata, not payloads.** At TB-scale, backfill files are enumerated (e.g. `s5cmd`) and handed to the bounded job directly. Kafka is the live notification bus; it is never a data bus.
- **Iceberg, not raw Parquet piles.** Checkpoint-aligned commits give atomicity, exactly-once semantics, schema evolution and hidden partitioning. Tables: `hypercore.{fills, order_statuses, raw_book_diffs, snapshots}`.

**Infra placement rule:** infra is defined by the first layer that needs it, and promoted to `platform/` when a second layer consumes it. Kafka therefore lives in this layer's `compose.yaml` for now; when transformation/serving start consuming its topics directly, its definition moves to the platform stack unchanged.

**Running the layer** — this directory is a standalone compose project (also included by the root stack):

```bash
docker compose up                      # kafka (KRaft) + topic bootstrap — the always-on slice
docker compose --profile node up       # + hyperdata-node (or replay tap) + sidecar → kafka
docker compose --profile warehouse up  # + iceberg-catalog (Nessie) + minio (S3-compatible dev storage)
```

Services: `kafka` (in-network `kafka:29092`, host tools `localhost:9092`), `kafka-init` (creates `hyperliquid.node-files`), `hyperdata-node` + `hyperdata-node-sidecar` (profile `node`), `iceberg-catalog` + `minio` (profile `warehouse`). Data lands on shared volumes `node-outputs`, `warehouse-data`.

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
| Sidecar (watcher → Kafka) | `ingestion/hyperdata-node-sidecar` | Python |
| Parse → Parquet → Iceberg job | `ingestion/hyperdata-ingestion-flink/parse-node-outputs` | Flink (Java fat-jar) |
| Iceberg maintenance (compaction, snapshot expiry) | `../transformation/spark` | Spark / Trino, Airflow-scheduled |

**Milestones (v0.0.1):**

- [ ] Sidecar: file watcher → Kafka topic `hyperliquid.node-files` (Kafka in KRaft mode via docker-compose).
- [ ] Flink job `parse-node-outputs`, bounded mode over a corpus window (replay-as-backfill PoC).
- [ ] Replay script: schedule copies of recorded hourly outputs into the watched directory.
- [ ] Iceberg REST catalog + `hypercore.*` tables in compose; Parquet writes with schema evolution.
- [ ] Live unbounded mode: Kafka source → checkpoint-committed Iceberg writes, adaptive file sizing.
- [ ] Snapshot-aligned backfill PoC with trailing-snapshot validation (one historical day).
- [ ] Stateful pre-modeling PoC: L4 book reconstruction + balance/position delta apply in Flink state.

---

## HyperEVM ingestion (ethereum-etl + Envio)

HyperCore gets the bespoke deltas+snapshots pipeline above; HyperEVM is a standard EVM chain (chain id 999) exposing Ethereum JSON-RPC, so it reuses **[ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)** — the generic EVM ETL toolchain behind BigQuery's `crypto_ethereum` dataset — instead of custom ingestion:

- **Source**: local `hyperdata-node` with `--serve-eth-rpc`, or any RPC provider (Dwellir, QuickNode, ...).
- **Backfill**: batch extraction over block ranges (`ethereumetl stream` / `extract`), landing the standard raw tables — `blocks`, `transactions`, `logs`, `traces`, `contracts`, `tokens`, `token_transfers`, `balances` — as Parquet/Iceberg, mirroring the BigQuery `crypto_ethereum` schema.
- **Real-time**: the same toolchain in head-following mode (`ethereumetl stream --start-block <head>`), writing identical tables — same commit duality as HyperCore: batch is range-aligned, live is time/size-aligned.
- **Division of labor with the Envio indexer**: ethereum-etl owns **generic raw EVM tables** (schema known upfront, all chains); [`hyperdata-indexer-hyperevm`](hyperdata-indexer-hyperevm/) owns **decoded events** (wildcard `Transfer`/`Approval` → Postgres via Envio HyperIndex). Raw first, decode later — the same principle as the HyperCore pipeline.

See the [indexer spec](hyperdata-indexer-hyperevm/docs/full_indexer_spec.md) for the phased raw-table design (Phase 1 firehose → Phase 2 decoded via dbt → Phase 3 traces/balances).

---

## Other ingestion services

| Service | What it does |
| :--- | :--- |
| [`hyperdata-node`](hyperdata-node/) | HyperLiquid node (hl-visor) emitting raw output files + periodic full-state snapshots; replay script (planned). |
| [`hyperdata-indexer-hyperevm`](hyperdata-indexer-hyperevm/) | Envio HyperIndex event firehose for HyperEVM (chain 999) → Postgres (decoded events); raw EVM tables via ethereum-etl — see above. |
| `hyperdata-ingestion-flink/` | Ingestion-stage Flink jobs (`parse-node-outputs`: JSONL → Parquet → Iceberg). |
| `hyperdata-node-sidecar` (planned) | File watcher → Kafka notifications. |
| `socket-listeners` (future) | WebSocket feeds from other CEX/DEX venues. |

---

# TODO | WIP: buildout runway

Execution plan for the ingestion layer, ordered by **dependency, not by component** — the goal is the shortest path to a demo-able end-to-end slice:

> one command (`make slice` / `scripts/local-run.sh`) → include-based compose (`node` + `warehouse` profiles) → replay tap drips files from the **real-data corpus** (S3-derived) → sidecar publishes to Kafka → Flink commits Parquet into Iceberg → query `hypercore.*` tables and see real rows.

Decisions already locked (do not re-litigate while executing): Java fat-jar for production jobs (volume + connector maturity; PyFlink only for samples/showcase) · Kafka notifications only, never payloads · replay = same path as live · finalization-only sidecar notifications · Iceberg owns Parquet writing (no bespoke parquet code) · `.gitmodules`-managed repos per service, plain dirs for dev tools · **real data over synthetic** — an S3 copy of actual node outputs already exists and is the primary dev/staging replay source.

---

## Phase 0.5 — Real-data inventory (BLOCKS topic & schema design) [next up]

**Goal:** inspect the S3 copy of actual node outputs and design topics/schemas from **observed reality**, not docs alone.

- [ ] Access check: creds + tooling on the dev VM (`aws` / `s5cmd` / `mc`), bucket name, requester-pays status.
- [ ] Inventory: directory tree (which tables actually exist), hourly layout vs documented, `periodic_abci_states` cadence in practice, presence of `replica_cmds` / `misc_events`.
- [ ] Ground-truth schemas: sample records per table — validate the documented L1 shapes (fills, order statuses, raw book diffs, misc events), note drift (missing fields, unexpected ones).
- [ ] Sizing reality: per-table GB/hour, records/block, file counts/day — replaces the "~100 GB/day" docs estimate; feeds Flink writer sizing (128–512 MB targets) and sidecar checksum cost estimates.
- [ ] Hour-rollover behavior: how the current hour's file actually grows and finalizes (appends? per-block writes? final rename?) — finalization heuristic in Phase 2 must match observed behavior.
- [ ] **Topic design decision** (outputs of this phase): single `hyperliquid.node-files` vs per-table topics; partition count & message keys from observed table cardinality; retention estimate.
- [ ] **Corpus selection**: pick a small contiguous window (e.g. 2–3 hours, ideally spanning a snapshot boundary) to become the dev/staging replay corpus; document its path.

**Acceptance:** a written inventory (this README or a linked note) with real schemas, real sizes, real rollover behavior, and the topic design recorded as locked decisions.

---

## Phase 0 — Kafka smoke test [parallel with 0.5]

**Goal:** prove the compose stack works before anything is built on it.

- [ ] `docker compose up` — kafka (KRaft) + kafka-init come up healthy; `kafka-init` exits 0.
- [ ] Verify topic: `kafka-topics.sh --bootstrap-server kafka:29092 --describe hyperliquid.node-files` (expect 6 partitions; **final count/keying per Phase 0.5 outcome**).
- [ ] Console round-trip on the in-network listener: produce one JSON notification, consume it back (`kafka:29092`).
- [ ] Console round-trip on the host listener (`localhost:9092`) — proves the dual-listener config for host-run tooling.
- [ ] Confirm topic auto-config: `KAFKA_AUTO_CREATE_TOPICS=true` is dev-only; note for prod flip.

**Acceptance:** a message produced inside the compose network is consumable both in-network and from the host.

---

## Phase 1 — Replay tap over the real corpus (+ tiny CI fixtures)

**Goal:** a tap that drips **real** node outputs into the watched volume, so everything downstream is built and tested against ground-truth data with no live node and no cloud dependency at run time.

**Corpus** (`corpus/`, plain dir — dev data, gitignored):

- [ ] Download the Phase-0.5-selected window from S3 into a local corpus dir (preserving the tree).
- [ ] Verify corpus integrity (checksums vs S3, record counts per file).
- [ ] Optionally trim: keep whole hours only, so finalization semantics see clean boundaries.

**Replay tap** (`replay-tap/`, plain dir — dev/staging tool, does not earn a submodule):

- [ ] Tiny Python service: copies corpus files into the watched `node-outputs` volume at an env-configurable wall-clock cadence (`REPLAY_DIR`, `REPLAY_CADENCE`, `REPLAY_SPEED`).
- [ ] Simulates hour rollover by advancing through the corpus's date/hour dirs — the sidecar's finalization logic sees the same file lifecycle the live node produces (per Phase-0.5 observations).
- [ ] Env-config only in v0 — **HTTP trigger endpoint deferred** (see parking lot).
- [ ] Compose wiring: `replay-tap` service under `node` profile as the alternative to `hyperdata-node` (either/or via `profiles`, sharing the `node-outputs` volume).
- [ ] Source flexibility: local corpus dir in v0; the same tap reads from the S3 bucket directly in staging (config switch only).

**CI fixtures** (`fixtures/`, plain dir — demoted from primary to CI/unit-test role):

- [ ] Small deterministic synthetic set (seeded generator, minutes of fake data, a few MB) for fast unit tests of sidecar/Flink without any corpus dependency.
- [ ] Real-shape records guaranteed by generating against the Phase-0.5 ground-truth schemas.

**Acceptance:** with only kafka + tap running, real files appear in the volume at the configured cadence with the correct hourly layout; CI fixtures fit in a seconds-scale test run.

---

## Phase 2 — Sidecar MVP (`hyperdata-node-sidecar`)

**Goal:** watcher → Kafka notifications + local backup, with the contract finalized below.

**Contract (lock these before coding):**

- [ ] Discovery: **full re-scan on startup** (idempotent re-emit of every finalized file) so component start order never matters; steady-state: watch loop (inotify where available, size/mtime polling fallback — inotify does not fire reliably on bind mounts/volumes).
- [ ] Finalization semantics: **notify on finalized files only** (hour rolled over / stable-size heuristic, e.g. unchanged size across N polls). The current hour's still-growing file is *not* notified. Latency ≤ 1 hour by design; low-latency tailing is the future stateful Flink tier, not the sidecar.
- [ ] Notification payload: `{table, path, date, hour, block_range, size, sha256, finalized_at}` — sha256 computed by the sidecar, trusted downstream.
- [ ] Kafka message key: `table + date + hour` → per-partition ordering per table.
- [ ] Delivery: **at-least-once** with retries; consumers are idempotent (deterministic keys), so duplicates are safe.
- [ ] Table set: `node_fills`, `node_order_statuses`, `node_raw_book_diffs`, `misc_events` + `periodic_abci_states` (as archive-only notification).

**Backups ("save to SSD, GCS, AWS or others"):**

- [ ] Pluggable sink interface (`BackupSink`): `backup(local_path) -> remote_ref`.
- [ ] v0 implementation: local SSD copy (same volume, `backups/` prefix) — enough to define and test the interface.
- [ ] GCS / S3 implementations slot behind the same interface **later** (parking lot) — no cloud SDKs in the MVP.

**Others (the "and more"):**

- [ ] Prometheus `/metrics`: files_seen, bytes_copied, publish_success/failure, watcher_lag_seconds, last_finalized_hour.
- [ ] Structured logs with the same fields as the notification payload (one line per event).
- [ ] Graceful shutdown: finish in-flight publish, then exit (clean compose restarts).

**Engineering:**

- [ ] Language: Python (watchdog/polling + kafka-python or confluent-kafka).
- [ ] Compose: `node` profile, `depends_on: kafka[healthy]`, mounts `node-outputs` at `WATCH_DIR`.
- [ ] Tests: unit (finalization heuristic, checksum, key derivation) + compose integration (tap → sidecar → assert messages on topic with correct payload).
- [ ] Sidecar README updated with the finalized contract.

**Acceptance:** every file the tap drips produces exactly one (or more, at-least-once) well-formed notification on `hyperliquid.node-files`, verifiable sha256, in table/hour order; `backups/` contains the copied files.

---

## Phase 3 — Flink bounded PoC (corpus dir → Parquet → Iceberg, no Kafka)

**Goal:** validate the **sink path alone** — catalog, storage, schemas, commits — with zero moving parts (`--source files` over the corpus dir; Kafka deliberately out of the loop).

**Cluster (`platform/flink-cluster`):**

- [ ] Bump image `flink:1.18` → current stable (2.x line); real `conf/`: `taskmanager.numberOfTaskSlots`, modest taskmanager memory bump, rest binding. Cluster image stays **generic** — all connectors live in the job jar.

**Warehouse (compose `warehouse` profile):**

- [ ] Nessie REST catalog: boots, reachable at `http://iceberg-catalog:19120/api/v1` (or current endpoint), config in `flink-conf`/catalog properties.
- [ ] MinIO: bucket create (e.g. `hyperdata-warehouse`), lifecycle sane for dev; S3 endpoint + path-style access wired into catalog + job configs.

**Job skeleton (`hyperdata-ingestion-flink/parse-node-outputs`, Java fat-jar):**

- [ ] Maven layout with shade plugin → one self-contained jar per job version (kafka-connector + flink-iceberg + iceberg-runtime bundled).
- [ ] Bounded entry mode: `--source files --input <dir>` → enumerate corpus files → parse → sink.
- [ ] Typed rows: POJOs per table mirroring the L1 schemas (see corpus ground-truth schemas from Phase 0.5), `block_time`/`block_number`/`log_index`/`transaction_hash` carried on every record.
- [ ] **Iceberg schemas (the design item):** DDLs owned by the job at startup (`CREATE TABLE IF NOT EXISTS`), source of truth = HL L1 data schemas doc:
  - `hypercore.fills`, `hypercore.order_statuses`, `hypercore.raw_book_diffs`, `hypercore.misc_events`;
  - hidden partitioning: `hours(block_time)` on hot tables (diffs, fills), `days(block_time)` on the rest;
  - Iceberg schema evolution from day 1 (additive columns are free).
- [ ] Sink = official `flink-iceberg` writer (upsert-free append; **Iceberg writes the parquet** — no bespoke parquet code anywhere).
- [ ] Deterministic row identity `(table, block_number, log_index)` for idempotency; re-running over the same corpus window is a no-op.
- [ ] Compose: `job-submitter` one-shot service pattern (`flink run` against the session JobManager, jar from a local build volume).

**Acceptance:** run bounded PoC over the corpus window → `SELECT COUNT(*)` per table via Spark/Trino (or Iceberg REST metadata read) matches the corpus record counts; files land in MinIO (corpus window will be small — sizing targets matter at production volume); re-run → no duplicates.

---

## Phase 4 — End-to-end: Kafka mode (unbounded)

**Goal:** close the loop — tap → sidecar → Kafka → Flink → Iceberg, the first true end-to-end run.

- [ ] `KafkaSource` on `hyperliquid.node-files`; notification → open file from shared storage → parse to EOF → file-completion watermark.
- [ ] Checkpoint-aligned Iceberg commits: interval 2–5 min, exactly-once; writer parallelism tuned so files land **128–512 MB** at production volume (adaptive sizing per the commit-duality table; corpus window will be smaller — parameterize, don't special-case).
- [ ] Startup reconciliation: job start does what the sidecar does — full re-scan of finalized-but-uncommitted files (start-order independence again).
- [ ] Replay == live verified: run the tap, then run the same corpus through bounded mode, diff the resulting Iceberg tables — must be identical.
- [ ] Failure drills: kill sidecar mid-run (no message loss beyond at-least-once), kill Flink TM mid-checkpoint (no partial commits in the table).
- [ ] **One-command local run:** `make slice` (or `scripts/local-run.sh`) — brings up the include-based compose (`node` + `warehouse` profiles), starts the tap against the corpus, submits the job, waits for the first commits, prints table row counts. The entire e2e slice must be a single command from a clean checkout.
- [ ] Compose: extend `job-submitter` with `--source kafka` mode; `make`/README updated.

**Acceptance:** `make slice` from clean → corpus drip ends as committed rows in `hypercore.*` queryable end-to-end; duplicates from at-least-once delivery are invisible (idempotent keys).

---

## Phase 5 — Stateful highlight reel (L2/L4 book "seal & check")

**Goal:** the showcase job — proves the stateful pre-modeling tier and de-risks the future low-latency path.

- [ ] Keyed state by `(coin, oid)`: apply `raw_book_diffs` in arrival order, maintain per-level book state in Flink state.
- [ ] **Seal the book:** on snapshot-file notification (Phase 2's `periodic_abci_states` events), re-baseline keyed state against ground truth, emit a `sealed_checkpoint` record (book hash, level counts, order counts, drift metrics).
- [ ] **Check correctness:** compare incrementally-built state vs. re-baselined state; emit drift telemetry (this is the "free correctness oracle" from the design, live).
- [ ] Language: PyFlink acceptable here (samples/showcase tier, iteration speed over throughput) — production core stays Java.
- [ ] Home: `hyperdata-ingestion-flink/` as a second job (`l4-premodeling` or under `samples/` first, promoted if it graduates).

**Acceptance:** a demo run showing book state building live, a snapshot arrival, and a `sealed_checkpoint` with zero (or reported) drift.

---

## Parking lot (deferred / to-discuss — do not build yet)

| Item | Note |
| :--- | :--- |
| Replay trigger endpoint (or sidecar-as-control-plane) | **To be discussed** — v0 uses env vars + compose restart; an HTTP control API (start/stop/speed/cadence) only when manual restarts actually hurt. Decision note: control-plane responsibility could live in the sidecar (it's the always-on component) rather than a third service. |
| Cloud backup sinks (GCS, S3) | Interface lands in Phase 2 (v0 = SSD copy); implementations when cloud creds/staging exist. |
| `.rmp` snapshot parsing (`translate-abci-state` integration) | v0: archive-only. Becomes load-bearing in the backfill segment work (validation oracle). |
| Snapshot-aligned backfill PoC (one historical day) | After live path is green — needs snapshot parsing + Airflow (dynamic task mapping) which is platform-layer scope. |
| Real `hyperdata-node` container build + live p2p run | Current placeholder keeps the volume warm; real image when we wire the actual node binary into compose. |
| `socket-listeners/` (other CEX/DEX venues) | Second source triggers the `services/sources/` reorg discussion + topic naming generalization (`<venue>.files`?). |
| Kafka → platform promotion | When transformation/serving consume Kafka directly (per infra placement rule). |
| Prod hardening | `KAFKA_AUTO_CREATE_TOPICS=false`, replication 3, SASL/mTLS, topic retention policy (notifications are short-lived: ~24–72h), schema registry (plain JSON now; Avro/Protobuf when a second consumer appears). |

---

## Dependency map

```
Phase 0.5 (S3 inventory) ──► topic design locked
        │
        ├─► Phase 0 (kafka smoke, parallel) ─┬─► Phase 1 (corpus + tap) ─► Phase 2 (sidecar) ─► Phase 4 (e2e + make slice) ─► Phase 5 (stateful)
        │                                    │                        │
        └────────────────────────────────────┴────────────────────────┴─► Phase 3 (bounded Flink PoC — needs corpus only, no sidecar)
```

Phase 3 needs Phase 1's corpus but not the sidecar — it proceeds **in parallel** with Phase 2. Phase 0.5 and Phase 0 are both quick and independent of each other; the first real engineering decisions surface in Phases 2 and 3.