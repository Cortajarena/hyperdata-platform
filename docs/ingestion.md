# Ingestion Layer

> **Last reviewed 2026-09-29.** Design decisions below marked *locked* are settled — extend them, don't
> re-litigate. Where reality contradicted an earlier assumption, the correction is recorded inline with
> the evidence, because the corpus is the authority (see [Phase 0.5](#phase-05-real-data-inventory-blocks-topic-schema-design-done-2026-09-21-schema-census-re-verified-2026-09-29)
> and [Ingestion schemas](#ingestion-schemas-the-contract-locked-2026-09-29)).

Everything between *a source emitting data* and *raw tables landing in the warehouse*: live nodes, sidecars, indexers, socket listeners and ingestion Flink jobs. Output of this layer is Parquet under Hive layout, managed as [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables.

```
hyperdata-node          hyperdata-indexer-hypercore     hyperdata-indexer-evm
   │   │                        │                              │
   │   └── snapshots            │ batch-polls the same         │ our fork of ethereum-etl
   ▼                            ▼ output files                 │ (chain 999 JSON-RPC)
JSONL appended continuously   parquet → iceberg                     ▼
   │                            hypercore.*                 blocks/txs/logs/traces/…
   ▼
hyperdata-node-sidecar (line tailer)
   │
   ├─► data-plane topics (per table, streamed lines):
   │     hyperliquid.book-diffs / .order-statuses / .fills   key: coin
   └─► seal topic (per finalized hour-file): hyperliquid.node-files
   ▼
jobs/flink/parse-node-outputs ──► Parquet + Iceberg (hypercore.* raw tables)

platform/schemas/ ──► ONE contract (protobuf) per table ──► both writers above,
                        generated Arrow schemas + JSON Schema, drift-checked in CI
```

---

## HyperCore pipeline design (deltas + full state snapshots)

HyperCore (and LOB / exchange chains in general) emits **stateful financial data**: per-event deltas (raw book diffs, order statuses, fills) plus periodic **full-state snapshots** (`periodic_abci_states/<date>/<height>.rmp` every ~10k blocks). Critically, HyperLiquid snapshots contain the **entire blockchain state — order books, account balances, margin positions, vaults — not just book state**. This duality shapes the whole pipeline.

```
┌─────────────────── RUNTIME (identical in live / replay / backfill) ───────────────────┐
│                                                                                       │
│  hyperdata-node (hl-visor) — raw JSONL outputs, appended continuously                 │
│   ├─ live:   p2p stream appends to hourly/<date>/<hour> files (~20 MB/s combined)     │
│   ├─ replay: tap progressively appends recorded files — indistinguishable downstream  │
│   └─ node also persists periodic_abci_states snapshots (~10k blocks)                  │
│                                                                                       │
│  hyperdata-node-sidecar (line tailer)                                                 │
│   ├─ tails growth (poll ~250 ms), reads appended bytes, tracks line boundaries        │
│   ├─ data plane: one message per new LINE ──► per-table topics                        │
│   │    hyperliquid.book-diffs / .order-statuses / .fills   (key: coin)                │
│        {table, path, date, hour, block_range, size, sha256, finalized_at}             │
│        (durability watermark + backup trigger; latency lives in the data plane)       │
│   └─ seal plane: one message per finalized HOUR-FILE ──► hyperliquid.node-files       │
│                                                                                       │
│  flink-job: parse-node-outputs (one job, unified bounded/unbounded)                   │
│   ├─ --source kafka   live + replay: stream lines, checkpoint-aligned commits         │
│   ├─ --source files   backfill: bounded enumeration of a file list (S3/SSD)           │
│   └─ core: JSONL → typed rows → Parquet → Iceberg commit                              │
│                                                                                       │
│  storage: Parquet, Hive layout (date/hour) + Apache Iceberg tables                    │
│   ├─ local dev: shared SSD volume       ├─ prod: S3 / GCS                             │
│   └─ catalog: REST catalog (Nessie / Polaris class) — one catalog for all engines     │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

**Why this shape:**

- **Line streaming, not file batching.** The node appends continuously; waiting for hour-file seals would add up to 1h latency and force 52 GB commit units. New lines flow to Kafka as appended (data plane); hour-file **seals** stay as a low-rate auxiliary plane for durability/reconciliation and backups. Kafka *is* a data plane here — ~20 MB/s sustained, routine Kafka load, sized accordingly.
- **Replay = live, exactly.** The replay tap progressively appends recorded outputs into the watched tree — same growth pattern as live, downstream cannot distinguish. One code path tests the entire pipeline.
- **Backfill = same job, different source.** Flink's bounded mode (`--source files`) runs the identical parse core. At TB-scale, backfill files are enumerated (e.g. `s5cmd`) and handed to the bounded job directly — Kafka never carries bulk reprocessing.
- **Iceberg, not raw Parquet piles.** Checkpoint-aligned commits give atomicity, exactly-once semantics, schema evolution and hidden partitioning. Tables: `hypercore.{fills, order_statuses, raw_book_diffs, snapshots}`.

**Infra placement rule (updated 2026-09-24):** *cluster systems* (Kafka, PostgreSQL, the Flink cluster — always-on infra no layer owns) are defined once under `platform/` and included by the stacks that consume them. Layer-scoped artifacts stay in `services/` (deployables) and `jobs/` (compute workloads). Kafka lives in [`platform/kafka/`](https://github.com/Cortajarena/hyperdata-platform/tree/main/platform/kafka) (KRaft broker + topic bootstrap, compose fragment included by the services slice); cross-layer execution plans live in `docs/`.

**Running the slice** — the services slice is standalone (`cd services && docker compose up`) and also included by the root stack; Kafka comes from the `platform/kafka` include, workloads are profile-gated:

```bash
docker compose up                      # kafka (KRaft) + topic bootstrap — the always-on slice
docker compose --profile node up       # + hyperdata-node (or replay tap) + sidecar → kafka
docker compose --profile warehouse up  # + iceberg-catalog (Nessie) + minio (S3-compatible dev storage)
```

Services: `kafka` (in-network `kafka:29092`, host tools `localhost:9092`) + `kafka-init` — defined once in [`platform/kafka/`](https://github.com/Cortajarena/hyperdata-platform/tree/main/platform/kafka), included here; `hyperdata-node` + `hyperdata-node-sidecar` (profile `node`); `iceberg-catalog` + `minio` (profile `warehouse`). Data lands on shared volumes `node-outputs`, `warehouse-data`.

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
| **Schema contract** (one `.proto` per table → Arrow + JSON Schema) | `platform/schemas/` | Protobuf / buf |
| Replay tap (progressive append) | `services/replay-tap/` (planned) | Python |
| Sidecar (line tailer → Kafka) | `services/hyperdata-node-sidecar` | Python |
| HyperCore indexer (batch-polls node output files → Iceberg) | `services/hyperdata-indexer-hypercore` | Python (pyiceberg) |
| Parse → Parquet → Iceberg job | `jobs/flink/parse-node-outputs` | Flink (Java fat-jar) |
| EVM indexer (our pinned fork of `ethereum-etl`) | `services/hyperdata-indexer-evm` | Python |
| Iceberg maintenance (compaction, snapshot expiry) | `jobs/spark` | Spark / Trino, Airflow-scheduled |

**Milestones (v0.1.0):**

- [ ] **Schema contract** `platform/schemas/`: one `.proto` per table, generated Arrow + JSON Schema committed, CI drift check.
- [ ] Sidecar (line tailer): appended lines → per-table data-plane topics + hour-file seals on `hyperliquid.node-files` (Kafka in KRaft mode via docker-compose).
- [ ] HyperCore indexer MVP: batch-poll hour-files → normalise → Arrow → Iceberg (`hypercore.{raw_book_diffs, order_statuses, fills, block_info}`), file ledger for idempotency.
- [ ] Flink job `parse-node-outputs`, bounded mode over a corpus window (replay-as-backfill PoC).
- [ ] Replay script: schedule copies of recorded hourly outputs into the watched directory.
- [ ] Iceberg REST catalog + `hypercore.*` tables in compose; Parquet writes with schema evolution.
- [ ] Live unbounded mode: Kafka source → checkpoint-committed Iceberg writes, adaptive file sizing.
- [ ] Snapshot-aligned backfill PoC with trailing-snapshot validation (one historical day).
- [ ] Stateful pre-modeling PoC: L4 book reconstruction + balance/position delta apply in Flink state.

---

## Ingestion schemas — the contract (locked 2026-09-29)

Every table that lands in the warehouse is described **once**, as a Protobuf contract, in `platform/schemas/`. That single contract feeds both writers (the Python indexer over files, the Flink job over Kafka) and produces both artifact kinds: **Arrow schemas** the writers load, and **JSON Schema** for non-Python consumers and CI. Nobody hand-writes a schema downstream — including the Flink job's Java row classes, which `protoc` generates from the same `.proto`.

Precedent: the EVM fork already ships DDL (`schemas/aws/*.sql`), so "schema as a checked-in artifact" is not a new idea here — it is the same discipline, applied to HyperCore.

### One contract per table (locked)

The three streaming tables do **not** share an event shape (verified against the corpus, not assumed):

| Wire file (`hourly/<date>/<hour>`) | Event encoding inside `events[]` | Table |
| :--- | :--- | :--- |
| `node_raw_book_diffs_streaming` | JSON object | `hypercore.raw_book_diffs` |
| `node_order_statuses_streaming` | JSON object with a nested, **recursive** `order` | `hypercore.order_statuses` |
| `node_fills_streaming` | **positional tuple** `["0x…", {…}]` — `user` is element 0 | `hypercore.fills` |
| *(derived, not a wire file)* | one row per block | `hypercore.block_info` |

Shared envelope: `{local_time, block_time, block_number, events[]}`. The Jun-10 capture has **exactly one event per line** (verified over 100k lines per table), but the contract must tolerate `N` — the Flink job and the indexer both explode it, so `N > 1` must not break anything.

A real line, verbatim (`node_raw_book_diffs_streaming/hourly/20260610/13`):

```json
{"local_time":"2026-06-10T13:10:56.723695057","block_time":"2026-06-10T12:56:15.035481067","block_number":1030140001,"events":[{"user":"0x7a146ee9d212816c31a0a93531e23d4359aade10","oid":464679247366,"coin":"LIT","side":"B","px":"1.5047","raw_book_diff":{"update":{"origSz":"7.0","newSz":"0.0"}}}]}
```

Note the two clocks: `block_time` is constant across a block, `local_time` advances ~40 µs per line *within* it. Files are partitioned by `local_time` (the 13:xx hour contains 12:56 chain time) — which is exactly why both must be carried.

### Row model: one row per event (locked)

The line is the *wire* unit; the **event** is the *row* unit. The normaliser broadcasts envelope fields down and emits one flat row per event. This is what makes `(block_number, log_index)` well-defined as an ordering coordinate, and it keeps the tables queryable without `UNNEST` in Trino/BigQuery.

```
line  {local_time, block_time, block_number, events:[e0, e1, …]}
   ──normalise──►  row(e0) = {envelope fields} + {e0 fields} + {log_index: 0}
                   row(e1) = {envelope fields} + {e1 fields} + {log_index: 1}   (log_index is a
                                                                               running counter across
                                                                               all lines of the block)
```

`log_index` is a real counter, not a formality: lines per block run to **812 median / 3,443 max** for book diffs and **1,243 median / 3,886 max** for order statuses. It is also the same thing the sidecar already calls `seq_in_block` in its line-id header — one concept, two names, and they must agree.

### Verified field contract

Wire types, null rates and nullability below are measured, not designed: 100,000 lines per table from `hourly/20260610/13` (the first ~1.5 h of that hour), plus targeted runs of 200k lines for the recursive-order checks. `string` for decimals is the Phase-0.5 rule (resolved in dbt staging); `timestamp('us', UTC)` is **mandatory** for `block_time`/`local_time` because Iceberg's `hours()`/`days()` hidden partitioning only applies to temporal types.

**`hypercore.raw_book_diffs`** — from `node_raw_book_diffs_streaming`

| Field | Wire | Null | Arrow | Note |
| :--- | :--- | :--- | :--- | :--- |
| `block_number` | int | 0% | `int64` | |
| `block_time` | str (ISO, ns) | 0% | `timestamp('us', UTC)` | partition key |
| `local_time` | str (ISO, ns) | 0% | `timestamp('us', UTC)` | ingestion-lag signal, per **line** |
| `log_index` | *(derived)* | — | `int64` | running counter within the block |
| `user` | str | 0% | `string` | |
| `oid` | int | 0% | `int64` | |
| `coin` | str | 0% | `string` | ~40% namespaced (`xyz:*`) → stays a string |
| `side` | str | 0% | `string` | |
| `px` | str | 0% | `string` | decimal |
| `diff_type` | *(derived)* | — | `string` | closed set: `new` \| `update` \| `remove` |
| `orig_sz` | str | — | `string` | `update` only |
| `new_sz` | str | — | `string` | `new` / `update` |

Distribution that makes the union load-bearing (100k lines): `new` 49,690 · **`"remove"` 49,518** · `update` 792. **Half the table is the bare-string variant** — see "closed set" below.

**`hypercore.order_statuses`** — from `node_order_statuses_streaming`

| Field | Wire | Null | Arrow | Note |
| :--- | :--- | :--- | :--- | :--- |
| `block_number` / `block_time` / `local_time` / `log_index` | as above | 0% | as above | |
| `user` | str | 0% | `string` | |
| `status` | str | 0% | `string` | 11 values (6 rejection types) → string |
| `hash` | str | **66.6%** | `string` | nullable |
| `builder` | object `{b: str, f: int}` | **99.3%** | struct | *object here, string in fills* |
| `order.oid` / `order.coin` / `order.side` | int / str / str | 0% | `int64` / `string` / `string` | |
| `order.limitPx` / `order.sz` / `order.origSz` / `order.triggerPx` | str | 0% | `string` | decimals |
| `order.timestamp` | int | 0% | `int64` | ms epoch |
| `order.orderType` / `order.triggerCondition` | str | 0% | `string` | 4 / n values → strings |
| `order.tif` | str | 0.1% | `string` | null exactly for market orders (`Stop Market`, `Take Profit Market`) |
| `order.cloid` | str | 11.4% | `string` | 16-byte hex |
| `order.isTrigger` / `order.isPositionTpsl` / `order.reduceOnly` | bool | 0% | `bool` | |
| `order.children` | list (**recursive**) | 0% (usually empty) | `list<struct<…>>` bounded to depth 3 | see below |
| ~~`events[].time`~~ | str | 0% | **dropped** | verified duplicate of `block_time` — 0 mismatches in 50,000 lines |
| `source_file` / `ingest_ts` | *(derived)* | — | `string` / `timestamp(us)` | provenance, see identity |

**`hypercore.fills`** — from `node_fills_streaming`

| Field | Wire | Null | Arrow | Note |
| :--- | :--- | :--- | :--- | :--- |
| `user` | **tuple element 0** | 0% | `string` | lifted out of the positional tuple |
| `time` | int | 0% | `int64` | epoch **ms** (contrast: statuses' time is an ISO string) |
| `coin` / `px` / `sz` / `side` / `dir` | str | 0% | `string` | `dir` has 9 values → string |
| `closedPnl` / `startPosition` / `fee` | str | 0% | `string` | decimals (incl. negatives) |
| `crossed` | bool | 0% | `bool` | |
| `hash` | str | 0% | `string` | 66 chars = `0x` + 64 hex (tx hash) |
| `oid` / `tid` | int | 0% | `int64` | `tid` 15 digits |
| `feeToken` | str | 0% | `string` | **not always USDC** (10+ aligned-quote tokens) |
| `cloid` | str | 27.9% | `string` | |
| `deployerFee` | str | 49.0% | `string` | |
| `builder` | str | 7.4% | `string` | *string here, object in statuses* |
| `builderFee` | str | 4.6% | `string` | |
| `twapId` | int | 5.7% | `int64` | |
| `priorityGas` | str | 2.0% | `string` | |
| `liquidation` | object `{liquidatedUser, markPx, method}` | 6 in 100k | struct | **missing from the Phase-0.5 census — corrected here** |
| `source_file` / `ingest_ts` | *(derived)* | — | `string` / `timestamp(us)` | |

A typical fill line carries 15 of these keys; the full possible set is 21 (the sparse ones plus `liquidation`). The Phase-0.5 census listed 16 fields and missed `liquidation`, `startPosition`, `time`, `deployerFee`, `builderFee`, `priorityGas` — this table is the corrected list.

### Normalisations the contract mandates

1. **Closed variant set.** `raw_book_diff` is `new{sz}` | `update{origSz,newSz}` | the bare string `"remove"`. The normaliser switches on the payload and emits `diff_type` + nullable `orig_sz`/`new_sz`. **A fourth variant is a contract violation → dead-letter + alert**, never a silent drop. This closed set is where the contract is actually enforced at runtime.
2. **Fills tuple flattening** → `user` + payload columns.
3. **Drop `order_statuses.events[].time`** (duplicate of `block_time`).
4. **Bounded recursion.** `order.children` nests child orders (trigger/TP-SL). Max depth observed: **2**, present in only **3 of 200,000** lines. The contract unrolls it to depth 3; anything deeper goes to `order_children_overflow_json` rather than being dropped — child orders matter for book reconstruction.
5. **Strings stay strings.** Decimals (`px`, `sz`, `origSz`, `newSz`, `limitPx`, `triggerPx`, `closedPnl`, `fee`, `startPosition`) and all category columns (`side`, `coin`, `status`, `orderType`, `tif`, `dir`, `triggerCondition`, `feeToken`, `diff_type`) stay `string`. Reasons: decimal precision, 9–11 category values, ~40% namespaced coins. Resolved in dbt, not at ingest.
6. **Timestamps become temporal types** (`timestamp('us', UTC)`), ns truncated — required for hidden partitioning.

### `hypercore.block_info` — the block-boundary table

One row per `(table, block_number)`. This is the "separate stream" for block-level data, and it doubles as the block-boundary watermark that `services/README.md` already requires ("watermark/barrier record is part of the topic contract from day one") and that snapshot-aligned backfill needs for its `[S_i, S_i+1)` segments.

| Field | Arrow | Meaning |
| :--- | :--- | :--- |
| `table` | `string` | which stream the block was seen in |
| `block_number` | `int64` | |
| `block_time` | `timestamp('us', UTC)` | chain time, partition key (`days`) |
| `first_local_time` / `last_local_time` | `timestamp('us', UTC)` | node-side span → ingestion lag for the block |
| `event_count` | `int64` | events exploded from this block |
| `log_index_min` / `log_index_max` | `int64` | contiguity check for the explode |
| `first_seen_at` | `timestamp('us', UTC)` | when the indexer first ingested it |
| `complete` | `bool` | `true` once the next block's first line is seen, or at hour-file end |

A block marked `complete` is safe to snapshot-align against. Rows are written in the **same transaction** as their events (pyiceberg's transaction API appends to both tables atomically), so `block_info` never claims completeness for events that were not committed.

### Identity & idempotency (locked 2026-09-29)

Row-level dedup on `(table, block_number, log_index)` — previously assumed — **does not survive reorgs**: the chain rewinds, the same `block_number` is re-emitted with different content, and there is no block hash anywhere in the L1 output to tell the two apart. Deduplicating on that key would silently discard the canonical block. (`block_number` is strictly monotonic across 100k lines per table in the Jun-10 capture, so this is a risk, not an observed event — it is exactly the kind that shows up in production.)

What replaces it:

- **Event coordinates** `(block_number, log_index)` — kept as columns: orderable, joinable, and the thing a consumer dedups on *within one file*.
- **Provenance** `source_file` + `ingest_ts` on every row.
- **Idempotency is file-level**, via a ledger of ingested hour-files keyed `(path, sha256)` — both values already exist in the sidecar's seal records. Re-running over the same file is a no-op; a reorg's *new* file is appended, not deduped away.
- **The raw layer keeps both sides of a reorg.** Choosing the canonical chain is a curated-layer concern: the existing 64-block confirmation buffer, applied in dbt.

This is what makes replays, backfills and reorgs compose instead of fighting: the unit of idempotency is the immutable artifact (the sealed file), not the mutable chain coordinate.

### Pipeline and hot path (measured, not estimated)

**Arrow cannot parse this wire format.** Tested directly against the corpus with `pyarrow.json.open_json` + `explicit_schema`:

| Attempt | Result |
| :--- | :--- |
| book diffs, union as `struct{update{}}` | `ArrowInvalid: /events/[]/raw_book_diff changed from object to string in row 6` |
| book diffs, union as `string` | fails the other way (`string → object in row 0`) |
| book diffs, union column omitted / envelope only | still fails (the reader walks the JSON regardless) |
| fills, `list<struct{user,fill}>` | `ArrowInvalid: /events/[] changed from object to array in row 0` |

So the normalise step stays in Python — it is the validator — and Arrow type-checks the **flat rows** against the generated schema. Measured on the real 15 GB book-diffs file (500k lines, 0 rejects):

| Stage | Throughput |
| :--- | :--- |
| `orjson.loads` + normalise | 335k lines/s per core |
| + column-wise `pa.array(col, type=…)` | **295k lines/s per core** |
| requirement: 15.6k + 23.5k + 0.2k lines/s | **~0.15 cores** |

Two implementation consequences: build columns as parallel Python lists and hand Arrow `pa.table({name: pa.array(col, type=…)})` rather than `Table.from_pylist` (the dict-per-row path is much slower); and buffer to 100–500 MB before appending, per the commit-strategy table above.

The Flink side gets the same contract for free: `protoc` generates the Java row classes, so the bounded and unbounded paths cannot drift from the indexer.

### Contract pipeline and CI — the fat note

```
platform/schemas/
├── README.md                        # this section's rules: regenerate, never hand-edit
├── buf.yaml / buf.gen.yaml          # or a `make schemas` target
├── proto/hypercore/v1/
│   ├── envelope.proto               # shared: local_time, block_time, block_number
│   ├── book_diffs.proto             # + the new/update/remove closed set
│   ├── order_statuses.proto         # + recursive Order, bounded depth
│   ├── fills.proto                  # + the positional [user, fill] wire form
│   └── block_info.proto
└── generated/                       # COMMITTED
    ├── arrow/*.json                 # what the indexer and the Flink job load
    └── json/*.json                  # contract artifact for non-Python consumers + CI
```

- **Generated files are committed**, so every consumer mounts a path and needs no `protoc`/`buf` at build or run time, and the schema is reviewable in a PR diff.
- **CI verifies, it does not produce:** it regenerates and fails if the committed output differs. That is what keeps "single source of truth" honest without making the repo depend on a toolchain.
- **Next step in CI:** `buf lint` + `buf breaking` (against the previous commit), which catches contract breaks across repos — the highest-value CI addition here, and the reason to standardise on `buf` rather than a bare `protoc-gen-jsonschema`.
- **The alternative, if we ever prefer not to commit generated code:** publish artifacts from CI and have consumers fetch them. Costs every consumer a `protoc`/`buf` toolchain and makes the schema invisible in review — viable, but it is a downgrade in auditability, so it stays a deliberate reversal rather than a drift.
- `shared/schemas/` is retired in favour of `platform/schemas/`: schemas are a cross-service contract, and `platform/` is where cluster systems and shared contracts live. (`shared/python` remains — it is a `uv` workspace member.)

---

## HyperEVM ingestion (our ethereum-etl fork + Envio)

HyperCore gets the bespoke deltas+snapshots pipeline above; HyperEVM is a standard EVM chain (chain id 999) exposing Ethereum JSON-RPC, so it reuses **[ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)** — the generic EVM ETL toolchain behind BigQuery's `crypto_ethereum` dataset — instead of custom ingestion. **We now own that fork** as [`hyperdata-indexer-evm`](https://github.com/Cortajarena/hyperdata-indexer-evm), pinned in `services/`: as of 2026-09-29 it is byte-identical to upstream `develop` (47 commits past the `v2.4.2` tag, all of them upstream's own), so the fork exists to *pin* the toolchain and give us somewhere to land HyperData-specific changes, not because it diverges yet. It tracks `develop` — the only submodule that does.

- **Source**: local `hyperdata-node` with `--serve-eth-rpc`, or any RPC provider (Dwellir, QuickNode, ...).
- **Backfill**: batch extraction over block ranges (`ethereumetl stream` / `extract`), landing the standard raw tables — `blocks`, `transactions`, `logs`, `traces`, `contracts`, `tokens`, `token_transfers`, `balances` — as Parquet/Iceberg, mirroring the BigQuery `crypto_ethereum` schema.
- **Real-time**: the same toolchain in head-following mode (`ethereumetl stream --start-block <head>`), writing identical tables — same commit duality as HyperCore: batch is range-aligned, live is time/size-aligned.
- **Division of labor with the Envio indexer**: the fork owns **generic raw EVM tables** (schema known upfront, all chains); [`hyperdata-indexer-hyperevm`](https://github.com/Cortajarena/hyperdata-indexer-hyperevm) owns **decoded events** (wildcard `Transfer`/`Approval` → Postgres via Envio HyperIndex). Raw first, decode later — the same principle as the HyperCore pipeline.

See the [indexer spec](https://github.com/Cortajarena/hyperdata-indexer-hyperevm/blob/main/docs/full_indexer_spec.md) for the phased raw-table design (Phase 1 firehose → Phase 2 decoded via dbt → Phase 3 traces/balances).

---

## Other ingestion services

| Service | What it does |
| :--- | :--- |
| [`hyperdata-node`](https://github.com/Cortajarena/hyperdata-node) | HyperLiquid node (hl-visor) emitting raw output files + periodic full-state snapshots. **Real image + live mode + pruner + snapshot-bootstrap tooling — done**; the node does *not* replay history (HL's L1 can't feed past blocks back), which is why replay is the ingestion layer's job. |
| [`hyperdata-indexer-hypercore`](https://github.com/Cortajarena/hyperdata-indexer-hypercore) | Indexes the node's output files by **batch polling** → Parquet/Iceberg. The file-path twin of the sidecar→Kafka→Flink path; both write the same `hypercore.*` tables from the same contract. (Planned; README only today.) |
| [`hyperdata-indexer-evm`](https://github.com/Cortajarena/hyperdata-indexer-evm) | **Our pinned fork of `ethereum-etl`** — generic raw EVM tables for chain 999 (and any EVM chain). Currently identical to upstream; the fork exists to pin and to hold HyperData changes. Tracks `develop`. |
| [`hyperdata-indexer-hyperevm`](https://github.com/Cortajarena/hyperdata-indexer-hyperevm) | Envio HyperIndex event firehose for HyperEVM (chain 999) → Postgres (**decoded** events); raw tables come from the fork above. |
| [`hyperdata-node-sidecar`](https://github.com/Cortajarena/hyperdata-node-sidecar) | **Line tailer** (contract written, implementation pending): tails appended lines → per-table data-plane topics keyed `coin` → hour-file seals. Not a "file watcher → notifications" job. |
| `platform/schemas/` | The one contract per table (`.proto` → generated Arrow + JSON Schema) that every writer above validates against. |
| `jobs/flink/` | Ingestion-stage Flink jobs (`parse-node-outputs`: JSONL → Parquet → Iceberg) over the Kafka stream; cluster lives in `platform/flink-cluster`. |
| `socket-listeners` (future) | WebSocket feeds for other CEX/DEX venues. |

---

# TODO | WIP: buildout runway

Execution plan for the ingestion layer, ordered by **dependency, not by component** — the goal is the shortest path to a demo-able end-to-end slice:

> one command (`make slice` / `scripts/local-run.sh`) → include-based compose (`node` + `warehouse` profiles) → replay tap drips files from the **real-data corpus** (local Jun-10 dump) → sidecar publishes to Kafka → Flink commits Parquet into Iceberg → query `hypercore.*` tables and see real rows.

Decisions already locked (do not re-litiate while executing): Java fat-jar for production jobs (volume + connector maturity; PyFlink only for samples/showcase) · **line streaming, not file batching — new lines flow to Kafka as appended; the warehouse path must not inherit hour-file latency or 52 GB commit units** · per-table data-plane topics (stateful/analytic consumers differ per table) + hour-file seals as a low-rate auxiliary topic · replay = same path as live (progressive append) · Iceberg owns Parquet writing (no bespoke parquet code) · `.gitmodules`-managed repos per service, plain dirs for dev tools · **real data over synthetic** — a local dump of actual node outputs (two hours, Jun 10) already exists at `/nvme0n1-disk/data/hl-node-data/` and is the primary dev/staging replay source.

---

## Phase 0.5 — Real-data inventory (BLOCKS topic & schema design) [DONE 2026-09-21; schema census re-verified 2026-09-29]

**Goal:** inspect actual node outputs and design topics/schemas from **observed reality**, not docs alone.

**We have a local dump of a real node's output: `/nvme0n1-disk/data/hl-node-data/data_stream_with_block_info/` — two consecutive hours (2026-06-10, hours 13–14), all three streaming tables + ten abci snapshots.** (~124 GB; a second May-29 one-hour capture sits alongside at `data_stream_with_block_info.20260531/`.)

- [x] Access check: **local, not S3** — dump lives at `/nvme0n1-disk/data/hl-node-data/` (344 GB total incl. scratch/derived artifacts; the platform-relevant part is `data_stream_with_block_info/`).
- [x] Inventory: three streaming tables only — `node_fills_streaming`, `node_order_statuses_streaming`, `node_raw_book_diffs_streaming`, each `hourly/<YYYYMMDD>/<HH>/<one file per hour>`; plus `periodic_abci_states/<date>/<height>.rmp`. **No `replica_cmds` / `misc_events` in the capture** — v0 table set is the three tables + snapshots.
- [x] Ground-truth schemas: `--batch-by-block` shape confirmed (`{local_time, block_time, block_number, events: [...]}`), one event per line in this capture; fills carry `px/sz/side/dir/closedPnl/hash/oid/crossed/fee/tid/cloid/feeToken/twapId`, order statuses carry full order objects (`oid, limitPx, sz, origSz, orderType, tif, ...`), book diffs carry `raw_book_diff: new{sz} | update{origSz,newSz} | "remove"`.
- [x] Sizing reality (per hour, Jun 10): **order_statuses ~52 GB/h** (84.5M lines, ~1,500 lines/block!) · book_diffs ~15 GB/h · fills ~0.5 GB/h · snapshots ~1.3 GB each. Block rate **~15.6 blocks/s** (56k blocks/h). Extrapolated: **~1.7 TB/day total, order_statuses alone ~1.2 TB/day** — an order of magnitude above the docs' "~100 GB/day"; sizing for Flink writers, Kafka rates and sidecar checksums starts from *these* numbers.
- [x] Hour-rollover behavior: one file per table per hour, appended during the hour, finalized at rollover → sidecar finalization = "hour dir no longer current", no per-block files.
- [x] **Topic design decision** (from the above): **per-table line-streaming topics + an hour-file seal topic** — locked; see "Kafka topic design" below.
- [x] **Schema census** (head/mid/tail sampling, Jun-10 files): all three tables are one-JSON-event-per-line `{local_time, block_time, block_number, events:[...]}`; book_diffs: 3 diff variants (`new` 47%, `remove` 51%, `update` 1.4%); fills: 16-field payload, sparse optionals (`cloid` 76%, `deployerFee` 61%, `builderFee` 3%, `builder` 5%, `priorityGas` 2%, `twapId` ~5% non-null), 9 `dir` values, **`feeToken` NOT always USDC** (10+ aligned-quote tokens); order_statuses: **11 `status` values** (6 rejection types), 5 `tif`, 4 `orderType`, `hash` null in 69%; **namespaced coins ≈ 40% of traffic** (`xyz:*`, `cash:*`, ...) → category columns stay strings. Raw-schema consequences: nullable everywhere, string-typed categories, px/sz as strings (decimals resolved in dbt staging), carry both `local_time` and `block_time` (free ingestion-lag telemetry).
- [x] **Census re-verified 2026-09-29** (100k lines/table + targeted 200k runs) — the field-level contract now lives in [Ingestion schemas](#ingestion-schemas-the-contract-locked-2026-09-29); this census is the summary. Corrections and additions it produced:
  - **Fills is not 16 fields.** The full set is 21 keys (a typical line carries 15); the census missed `liquidation{liquidatedUser, markPx, method}`, `startPosition`, `time` (epoch-ms **int**), `deployerFee`, `builderFee`, `priorityGas`.
  - **Fills events are a positional tuple** `["0x…", {…}]`, not an object — `user` is element 0. The other two tables use objects.
  - **`order.children` is recursive** (child/TP-SL orders): max depth **2**, present in **3 of 200,000** lines.
  - **`builder` differs per table**: an object `{b, f}` in order statuses (99.3% null), a plain string address in fills.
  - **`order_statuses.events[].time` duplicates `block_time`** (0 mismatches in 50,000 lines) — dropped from the row.
  - **Lines per block**: book diffs median 812 / max 3,443 · order statuses median 1,243 / max 3,886 · fills median 8 → `log_index` is a real per-block counter, not a formality.
  - **`block_number` is strictly monotonic** across all three 100k samples (no reorg observed in this window — but see the identity discussion in the schema section: reorgs still break row-level dedup).
  - **Arrow cannot parse the wire format** (`pyarrow.json` rejects both the diff union and the fills tuple) — the normaliser must be Python; measured 295k lines/s/core.

**Acceptance:** inventory + census + topic design written down (this section + the one below). Phase 0.5 complete.

---

## Kafka topic design (locked)

> **Source of truth:** [`platform/kafka/topics.yaml`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/kafka/topics.yaml) — machine-readable topic contract, applied idempotently by [`platform/kafka/init-topics.sh`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/kafka/init-topics.sh) through the `kafka-init` compose service (fragment in `platform/kafka/compose.yaml`). The same contract targets K8s later (rendered to Strimzi `KafkaTopic` CRs, or the script as a one-shot `Job`). The table below mirrors it; if they disagree, the YAML wins.

The sidecar **tails appended lines** — Kafka is a **data plane** for the three streams, plus a **seal plane** for durability. Per-table topics because processing diverges: fills → trade analytics; book diffs → (future) stateful book reconstruction; order statuses → lifecycle analytics. Different consumers, different state, 100× rate differences.

| Topic | Plane | Contents | Key | Rate (Jun-10 measured) | Partitions | Retention |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `hyperliquid.book-diffs` | data | diff lines, streamed | `coin` | ~15.6k lines/s, ~4.5 MB/s | 6 | 72h |
| `hyperliquid.order-statuses` | data | status lines, streamed | `coin` | ~23.5k lines/s, ~14 MB/s | 6 | 72h |
| `hyperliquid.fills` | data | fill lines, streamed | `coin` | ~200 lines/s, <1 MB/s | 3 | 72h |
| `hyperliquid.node-files` | seal | finalized hour-files: `{table, path, date, hour, block_range, size, sha256, finalized_at}` | `table\|date\|hour` | ~3/day | 3 | 72h |
| `hyperliquid.node-snapshots` | seal (parked) | `.rmp` snapshot events — design pending; snapshots will later serve as **book seals / health-check oracles** | height | ~4-6/h | 3 | TBD |

- **One record = one message** (no batch framing): ~40k msg/s combined is routine Kafka; per-record offsets map cleanly to Flink exactly-once. Values are raw node JSON lines, unchanged.
- **Key = `coin`** on data topics: per-coin ordered partitions — the future stateful book job gets all diffs for a coin in one partition, in order, for free.
- **Seals** trigger backups (whole, immutable, checksummed files), provide the reconciliation watermark ("durable ≤ here"), and give backfill its file-level dedup keys.
- **Sizing (critical):** combined ~20 MB/s sustained, ~1.6 TB/day raw (~5 TB broker disk for 72h retention, less with zstd — producers compress). Broker, partition and consumer-poll tuning must be sized from these numbers, not from the old "notification bus" assumption. Retention is a safety net, not the archive — Iceberg is the archive; seals prove it.
- **Latency budget:** node append → sidecar poll (250 ms) → Kafka → Flink → checkpoint commit (2–5 min) = seconds-to-minutes end to end; the seal plane exists for durability, not latency.
- [x] **Corpus selection**: the Jun-10 two-hour capture **is** the corpus — complete tree, spans ten snapshot boundaries; path documented here. (Optionally trim to a subset for faster CI.)

**Acceptance:** a written inventory (this section) with real schemas, real sizes, real rollover behavior — done. Remaining: topic design decision recorded as locked.

---

## Phase 0 — Kafka smoke test [DONE 2026-09-22]

**Goal:** prove the compose stack works before anything is built on it — now against the **locked four-topic design**.

- [x] `docker compose up` — kafka (KRaft) + kafka-init come up healthy; `kafka-init` exits 0.
- [x] Topic contract in [`platform/kafka/topics.yaml`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/kafka/topics.yaml); applied by [`platform/kafka/init-topics.sh`](https://github.com/Cortajarena/hyperdata-platform/blob/main/platform/kafka/init-topics.sh) (idempotent, awk-parsed — image ships no python/yq).
- [x] Create the four topics: `hyperliquid.book-diffs` (6p), `hyperliquid.order-statuses` (6p), `hyperliquid.fills` (3p), `hyperliquid.node-files` (3p, seals). (`node-snapshots` parked.)
- [x] Verify topics + partitions + `retention.ms=259200000` via `--describe`.
- [x] Console round-trip in-network (`kafka:29092`) and on the host listener (`localhost:9092`) — both pass.
- [x] Idempotency: re-running `kafka-init` is a no-op; topics survive broker recreate.

**Two bugs found and fixed during the smoke test (worth remembering):**
1. **Host listener unreachable** — `LOCAL://localhost:9092` *listens* on the container's loopback only, so the published port (DNAT to the container IP) couldn't reach it. Fix: listen on `0.0.0.0:9092`, advertise `localhost:9092`.
2. **Topics vanished on recreate** — the image's default data dir is `/tmp/kafka-logs` (container layer), not the mounted `/var/lib/kafka/data`. Fix: `KAFKA_LOG_DIRS=/var/lib/kafka/data`.

**Acceptance:** met — messages round-trip on all four topics both in-network and from the host; topic data persists across broker restarts.

---

## Phase 1 — Replay tap over the real corpus (+ tiny CI fixtures)

**Goal:** a tap that **progressively appends** real node outputs into the watched volume, so everything downstream is built and tested against ground-truth data with no live node — and with the same *growth pattern* a live node produces.

**Corpus** (the Jun-10 two-hour local dump; plain dir — dev data, gitignored):

- [ ] Point the tap at `/nvme0n1-disk/data/hl-node-data/data_stream_with_block_info/` (or copy the selected window into a smaller `corpus/` dir for CI speed).
- [ ] Verify corpus integrity (record counts per file vs the inventory numbers).

**Replay tap** (`replay-tap/`, plain dir — dev/staging tool, does not earn a submodule):

- [ ] Python service: **progressive append** — reads a corpus file in growing chunks (e.g. N MB per tick) and appends them to the target file in the watched volume, at an env-configurable cadence (`REPLAY_DIR`, `REPLAY_CADENCE`, `REPLAY_SPEED`). Whole-file copy is NOT replay: the sidecar tailer must see the same continuous append pattern live produces; chunked append also lets us test partial-line handling and offset crash-recovery.
- [ ] Simulates hour rollover: when a corpus hour-file is exhausted, rotate to the next hour (touch the seal boundary) and continue with the next file.
- [ ] Env-config only in v0 — **HTTP trigger endpoint deferred** (see parking lot).
- [ ] Compose wiring: `replay-tap` service under `node` profile as the alternative to `hyperdata-node` (either/or via `profiles`, sharing the `node-outputs` volume).
- [ ] Source flexibility: local corpus dir in v0; the same tap reads from the dump dir directly, and from an S3/GCS bucket in staging (config switch only).

**CI fixtures** (`fixtures/`, plain dir — CI/unit-test role):

- [ ] Small deterministic synthetic set (seeded generator, minutes of fake data, a few MB) for fast unit tests of sidecar/Flink without any corpus dependency.
- [ ] Real-shape records guaranteed by generating against the Phase-0.5 ground-truth schemas.

**Acceptance:** with only kafka + tap running, the watched volume shows corpus files growing by appends (not appearing whole), hour boundaries rotating on schedule; a tailing reader observes byte-growth in small increments with occasional partial (non-newline-terminated) chunk tails.

---

## Phase 2 — Sidecar MVP (`hyperdata-node-sidecar`) — line tailer

**Goal:** tail appended lines → per-table data-plane topics + hour-file seal topic + local backup. Full build spec: [`hyperdata-node-sidecar/README.md`](https://github.com/Cortajarena/hyperdata-node-sidecar/blob/main/README.md) (the contract below summarizes it).

**Contract (summary — details in the sidecar README):**

- [ ] **Tailer, not watcher**: poll growth (~250 ms default) on current-hour files per table; read appended bytes; track line boundaries (partial trailing line held back until newline arrives).
- [ ] **Data plane**: one Kafka message per new line → per-table topic, keyed `coin`. Values = raw node JSON lines, unchanged.
- [ ] **Stable line identity (idempotency contract)**: every message carries a stable line id — `table|block_number|seq_in_block` (preferred; semantic, survives re-publication) or the line's byte offset — as a Kafka header. The append-only parse job absorbs at-least-once duplicates, but **future stateful consumers cannot**: replaying an `update`/`remove` diff is not idempotent, so the line id is their dedup key. Cheap now, impossible to retrofit once topics are live. `seq_in_block` is the same counter the schema contract calls `log_index` — one concept, two names, and they must agree.
- [ ] **Seal plane**: on hour rollover, publish `{table, path, date, hour, block_range, size, sha256, finalized_at}` → `hyperliquid.node-files`, keyed `table|date|hour`.
- [ ] **Offset state**: per-file byte offsets persisted (after Kafka ack) to a local state store; crash → resume from offset → at-least-once (downstream exactly-once absorbs duplicates).
- [ ] **Startup reconciliation**: full scan of watched tree, resume from persisted offsets — start-order independent.
- [ ] **Backups**: pluggable `BackupSink` (`backup(local_path) -> remote_ref`); v0 = local SSD copy; seals are the trigger. GCS/S3 later (parking lot).
- [ ] **Observability**: Prometheus metrics (lines_published, bytes_read, publish_lag vs block_time, tail_offset_lag, seals_emitted, partial_line_held_bytes) + structured logs + graceful shutdown.
- [ ] **Hot path engineering**: ~20 MB/s, ~40k msg/s at Jun-10 rates — confluent-kafka producer with `idempotence=true`, `acks=all`, zstd, tuned linger/batch; per-table tailer threads.
- [ ] Tests: unit (line-boundary handling, offset persistence/resume, seal sha256, coin-key derivation) + compose integration (tap → sidecar → assert per-table messages + seal on rollover).
- [ ] Sidecar README kept as the canonical contract (already written; update as implementation lands).

**Acceptance:** with the tap appending, every appended chunk reaches the matching data-plane topic as messages (one per complete line, `coin`-keyed, in order); on hour rollover, exactly one seal per file lands on `hyperliquid.node-files` with correct sha256/block_range; kill the sidecar mid-tail → restart resumes from persisted offsets (re-published lines absorbed downstream); `backups/` contains sealed files.

---

## Phase 3 — Flink bounded PoC (corpus dir → Parquet → Iceberg, no Kafka)

**Goal:** validate the **sink path alone** — catalog, storage, schemas, commits — with zero moving parts (`--source files` over the corpus dir; Kafka deliberately out of the loop).

**Cluster (`platform/flink-cluster`):**

- [ ] Bump image `flink:1.18` → current stable (2.x line); real `conf/`: `taskmanager.numberOfTaskSlots`, modest taskmanager memory bump, rest binding. Cluster image stays **generic** — all connectors live in the job jar.

**Warehouse (compose `warehouse` profile):**

- [ ] Nessie REST catalog: boots, reachable at `http://iceberg-catalog:19120/api/v1` (or current endpoint), config in `flink-conf`/catalog properties.
- [ ] MinIO: bucket create (e.g. `hyperdata-warehouse`), lifecycle sane for dev; S3 endpoint + path-style access wired into catalog + job configs.

**Job skeleton (`jobs/flink/parse-node-outputs`, Java fat-jar):**

- [ ] Maven layout with shade plugin → one self-contained jar per job version (kafka-connector + flink-iceberg + iceberg-runtime bundled).
- [ ] Bounded entry mode: `--source files --input <dir>` → enumerate corpus files → parse → sink.
- [ ] Typed rows: **generated from the schema contract** (`protoc` → Java), not hand-written — the same contract the Python indexer uses, so the two writers cannot drift. `block_time`/`block_number`/`log_index`/`source_file`/`ingest_ts` carried on every record.
- [ ] **Iceberg schemas:** DDLs owned by the job at startup (`CREATE TABLE IF NOT EXISTS`), but the column set comes from the contract in [Ingestion schemas](#ingestion-schemas-the-contract-locked-2026-09-29) — not from a doc:
  - `hypercore.raw_book_diffs`, `hypercore.order_statuses`, `hypercore.fills`, `hypercore.block_info` (no `misc_events` in v0 — not in the capture);
  - hidden partitioning: `hours(block_time)` on hot tables (diffs, fills), `days(block_time)` on the rest and on `block_info`;
  - Iceberg schema evolution from day 1 (additive columns are free).
- [ ] Sink = official `flink-iceberg` writer (upsert-free append; **Iceberg writes the parquet** — no bespoke parquet code anywhere).
- [ ] Idempotency per the locked decision: **file-level ledger** keyed `(path, sha256)` → re-running over the same corpus window is a no-op. Not row-level dedup on `(table, block_number, log_index)` — that silently drops reorged blocks.
- [ ] Compose: `job-submitter` one-shot service pattern (`flink run` against the session JobManager, jar from a local build volume).

**Acceptance:** run bounded PoC over the corpus window → `SELECT COUNT(*)` per table via Spark/Trino (or Iceberg REST metadata read) matches the corpus record counts; files land in MinIO (corpus window will be small — sizing targets matter at production volume); re-run → no duplicates.

---

## Phase 3.5 — Schema contract + HyperCore indexer (file path, no Kafka)

**Goal:** the contract that both writers share, and the first writer built from it — the Python indexer that batch-polls the node's output files straight into Iceberg. Deliberately parallel to Phases 2/3: it needs the corpus and the warehouse, not the sidecar or Kafka. Design: [Ingestion schemas](#ingestion-schemas-the-contract-locked-2026-09-29).

**Contract (`platform/schemas/`)** — one `.proto` per table (`envelope`, `book_diffs`, `order_statuses`, `fills`, `block_info`), fields verified against the corpus:

- [ ] `buf` workspace + `make schemas` target: `.proto` → `generated/arrow/*.json` + `generated/json/*.json`, both **committed**.
- [ ] CI job: regenerate and **fail on drift**; then add `buf lint` + `buf breaking` against the previous commit.
- [ ] A `platform/schemas/README.md` stating the rule: edit the `.proto`, never the generated files.
- [ ] Unit tests pinning the closed sets the corpus proved: diff variants `{new, update, remove}`, `side ∈ {A,B}`, the fills tuple shape, `order.children` depth ≤ 3, unknown variant ⇒ DLQ (not a silent drop).

**Indexer (`services/hyperdata-indexer-hypercore`, Python)**:

- [ ] Load the generated Arrow schema once at startup; stream hour-files line by line — `orjson.loads` → normalise → column-wise `pa.array` (no dict-per-row).
- [ ] Enforce the contract at runtime: unknown diff variant / unexpected type ⇒ dead-letter + alert, with a DLQ directory and a rejected-line counter in metrics.
- [ ] Buffer to 100–500 MB, then `pyiceberg` append against the REST catalog (MinIO in dev) — events **and** the matching `block_info` rows in one transaction.
- [ ] **File ledger** keyed `(path, sha256)`: skip already-ingested files, so a re-run is a no-op and an interrupted run resumes per file.
- [ ] Watch mode for live use (poll for the current hour file, ingest incrementally) — same code path as the backfill mode, mirroring "replay = live".
- [ ] Metrics: lines in/out, rejects, bytes/s, lag vs `block_time`, files ingested, DLQ depth.
- [ ] Tests: normalise against real corpus lines (the three variants + the fills tuple + a nested `children` case), Arrow schema conformance, ledger idempotency, end-to-end into MinIO.

**Acceptance:** point the indexer at the Jun-10 hour → `hypercore.*` row counts match the corpus record counts; `block_info` reconciles (`event_count` per block = rows written, `log_index_min/max` contiguous); re-run → zero new rows; a deliberately corrupted line lands in the DLQ and the run continues.

---

## Phase 4 — End-to-end: Kafka mode (unbounded)

**Goal:** close the loop — tap → sidecar → Kafka → Flink → Iceberg, the first true end-to-end run.

- [ ] `KafkaSource` on the three data-plane topics (per-table source parallelism); lines arrive pre-parsed as single messages — no file-opening step, no file-completion watermark; the **seal topic is consumed separately** as a reconciliation watermark (drives Iceberg maintenance decisions and lag metrics).
- [ ] Checkpoint-aligned Iceberg commits: interval 2–5 min, exactly-once; writer parallelism tuned so files land **128–512 MB** at production volume (adaptive sizing per the commit-duality table; corpus window will be smaller — parameterize, don't special-case).
- [ ] Startup reconciliation: bounded re-scan of finalized-but-uncommitted files (start-order independence again).
- [ ] Replay == live verified: run the tap, then run the same corpus through bounded mode, diff the resulting Iceberg tables — must be identical.
- [ ] Failure drills: kill sidecar mid-run (no message loss beyond at-least-once), kill Flink TM mid-checkpoint (no partial commits in the table).
- [ ] **One-command local run:** `make slice` (or `scripts/local-run.sh`) — brings up the include-based compose (`node` + `warehouse` profiles), starts the tap against the corpus, submits the job, waits for the first commits, prints table row counts. The entire e2e slice must be a single command from a clean checkout.
- [ ] Compose: extend `job-submitter` with `--source kafka` mode; `make`/README updated.

**Acceptance:** `make slice` from clean → corpus drip ends as committed rows in `hypercore.*` queryable end-to-end; duplicates from at-least-once delivery are invisible (idempotent keys).

---

## Phase 5 — Stateful highlight reel (L2/L4 book "seal & check")

**Goal:** the showcase job — proves the stateful pre-modeling tier and de-risks the future low-latency path.

- [ ] Keyed state by `(coin, oid)`: apply `raw_book_diffs` in arrival order, maintain per-level book state in Flink state. `keyBy(coin)` distributes per-book processing — one subtask owns whole books.
- [ ] **Dedup on the stable line id** (Phase 2): the sidecar is at-least-once; diff replays are not idempotent, so the keeper must drop lines already applied. This is the consumer that makes the line-id contract load-bearing (see [Appendix](#appendix-stateful-l2-book-poc-future)).
- [ ] **Seal the book:** on snapshot-file notification (Phase 2's `periodic_abci_states` events), re-baseline keyed state against ground truth, emit a `sealed_checkpoint` record (book hash, level counts, order counts, drift metrics).
- [ ] **Check correctness:** compare incrementally-built state vs. re-baselined state; emit drift telemetry (this is the "free correctness oracle" from the design, live).
- [ ] Language: PyFlink acceptable here (samples/showcase tier, iteration speed over throughput) — production core stays Java.
- [ ] Home: `jobs/flink/` as a second job (`l4-premodeling` or under `samples/` first, promoted if it graduates).

**Acceptance:** a demo run showing book state building live, a snapshot arrival, and a `sealed_checkpoint` with zero (or reported) drift.

---

## Parking lot (deferred / to-discuss — do not build yet)

| Item | Note |
| :--- | :--- |
| Replay trigger endpoint (or sidecar-as-control-plane) | **To be discussed** — v0 uses env vars + compose restart; an HTTP control API (start/stop/speed/cadence) only when manual restarts actually hurt. Decision note: control-plane responsibility could live in the sidecar (it's the always-on component) rather than a third service. |
| Cloud backup sinks (GCS, S3) | Interface lands in Phase 2 (v0 = SSD copy); implementations when cloud creds/staging exist. |
| `.rmp` snapshot parsing (`translate-abci-state` integration) | v0: archive-only. Becomes load-bearing in the backfill segment work (validation oracle). |
| Snapshot-aligned backfill PoC (one historical day) | After live path is green — needs snapshot parsing + Airflow (dynamic task mapping) which is platform-layer scope. |
| Real `hyperdata-node` container build + live p2p run | **Mostly done 2026-09-26** — the node repo now has a real image (gpg-verified `hl-visor` + s5cmd), live mode with host networking and a fills tmpfs, a 48h pruner, and `scripts/bootstrap-snapshot/` (ephemeral EC2 state-sync). Remaining: wire that stack into the platform compose. Note the volume contract differs — the node bind-mounts `${DATA_DIR}/hl-node-data` → `/home/hluser/hl`, *not* the platform's `node-outputs` named volume, so the sidecar's `WATCH_DIR` has to point at that bind mount (this is the one real blocker for the e2e run). |
| Schema registry for the Kafka topics | Separate concern from the table contract. The topic payloads are raw node JSONL and the tables are governed by `platform/schemas/` (protobuf contract + generated Arrow/JSON Schema) — so a Kafka schema registry is *not* required for correctness here; revisit only when a second non-Python consumer appears (Avro/Protobuf on the wire). |
| `socket-listeners/` (other CEX/DEX venues) | Second source triggers the `services/sources/` reorg discussion + topic naming generalization (`<venue>.files`?). |
| Kafka → platform promotion | **Done 2026-09-24:** moved to [`platform/kafka/`](https://github.com/Cortajarena/hyperdata-platform/tree/main/platform/kafka) as a platform system (submitter-owned broker, not a layer service); services slice includes its compose fragment. Future consumers (transformation/serving) include the same fragment. |
| Prod hardening | `KAFKA_AUTO_CREATE_TOPICS=false`, replication 3, SASL/mTLS, topic retention policy (short-lived: ~24–72h). |

---

## Dependency map

```
Phase 0.5 (real-data inventory) ──► topic design locked
        │
        ├─► Phase 0 (kafka smoke, parallel) ─┬─► Phase 1 (corpus + tap) ─► Phase 2 (sidecar) ─► Phase 4 (e2e + make slice) ─► Phase 5 (stateful)
        │                                    │                        │
        │                                    └─► Phase 3.5 (schema contract + file-path indexer) ─┘
        │                                                     │  (needs corpus + warehouse only)
        └─────────────────────────────────────────────────────┴─► Phase 3 (bounded Flink PoC — needs corpus only, no sidecar)
```

Phase 3 needs Phase 1's corpus but not the sidecar — it proceeds **in parallel** with Phase 2. Phase 3.5 also runs in parallel: it needs the corpus and the `warehouse` profile, and it is the first consumer of the schema contract that Phase 3's job classes are generated from — so **build the contract first** (it is the input to both writers, and retrofitting it after the Flink job has hand-written POJOs means touching live tables). Phase 0.5 and Phase 0 are both quick and independent of each other; the first real engineering decisions surface in Phases 2 and 3.

---

# Appendix: stateful L2 book POC (future)

Forward reference for the first non-trivial **stateful** consumer — an L2 order-book keeper — for after v1, which is deliberately stateless (parse → Parquet/Iceberg only). Tracked as Phase 5 above.

**Why the architecture already supports it:**

- `hyperliquid.book-diffs` is keyed by `coin` → all lines for an instrument land in one partition, **in order**. Sequential state application, for free.
- Consumers are independent Kafka consumer groups: the book keeper runs beside the Iceberg parse job with its own lag and checkpoints. No coupling — a slow keeper never stalls the archive.
- `periodic_abci_states` snapshots provide ground truth to re-baseline and verify against.

**Shape of the POC:**

- **Source:** `hyperliquid.book-diffs` — L4 per-order deltas (`new{sz}` | `update{origSz,newSz}` | `remove`).
- **Keying:** `keyBy(coin)` → per-book distributed processing; each parallel subtask owns whole books.
- **State:** `MapState` of L2 levels `(coin, px) -> {sz, oids}` aggregated from L4 orders, plus per-`oid` bookkeeping so `update`/`remove` apply correctly.
- **Output:** derived streams (L2 snapshots, top-of-book, liquidity metrics) to their own topics and/or a serving store (ClickHouse/Redis) — independent of the raw archive.

**The two properties that make it more than a toy:**

1. **Seal & check (drift correction).** On each snapshot-seal, re-baseline computed state from the snapshot-derived book and diff against incrementally-built state → emit drift telemetry. Self-healing and self-verifying.
2. **Cold start without Kafka history.** Kafka retention bounds catch-up; the keeper bootstraps from the snapshot at height `H` and resumes from the offset for `H`. Snapshots + ordered diffs are the system of record for state — not Kafka.

**Constraints:**

- **Line idempotency** — the sidecar is at-least-once and `update`/`remove` are not idempotent, so the keeper must dedup on the stable line id (Phase 2). Harmless for the append-only parse job; load-bearing here.
- **State size** — L4 for all coins is heavy; L2 is far smaller. RocksDB backend + checkpointing to object storage; parallelize by coin activity.
- **Keying** — `coin` covers per-instrument books; any future cross-asset state (portfolio, index, cross-venue) needs a different key or a co-partitioned join.

**Sequencing:** after v1 (stateless parse → Iceberg). Phase 5's acceptance is a demo run showing book state building live, a snapshot arrival, and a `sealed_checkpoint` with zero (or reported) drift.