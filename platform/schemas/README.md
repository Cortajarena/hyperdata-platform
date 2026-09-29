# Ingestion schemas — the contract

One Protobuf contract for everything the HyperCore ingestion layer lands in the
warehouse, in `proto/hypercore/v1/`. Consumers never hand-write a schema: the
Python indexer and the Flink job both derive their row types from here.

The full design — row model, field-level contract, identity rules, measured
throughput — is in [docs/ingestion.md](https://github.com/Cortajarena/hyperdata-platform/blob/main/docs/ingestion.md#ingestion-schemas--the-contract-locked-2026-09-29).
This README is the operational contract: how to regenerate, and what the rules
are.

## Layout

```
proto/hypercore/v1/
  envelope.proto        shared: block_number, block_time, local_time + RowMeta
  book_diffs.proto      RawBookDiffRow  + the new/update/remove variant set
  order_statuses.proto  OrderStatusRow  + recursive Order (bounded)
  fills.proto           FillRow         + the positional [user, fill] wire form
  block_info.proto      BlockInfoRow    — derived, one row per (table, block)
generated/
  arrow/hypercore_arrow/*.py   pa.schema([...]) per table — what writers import
  json/*.json                  JSON Schema draft-07 — contract artifact
tools/
  generate_schemas.py           proto -> both artifacts, and the drift gate
  check_corpus_conformance.py   real corpus lines against the generated schema
```

## Commands

```bash
make generate      # regenerate generated/ from proto/, then commit the result
make check         # fail if generated/ has drifted (this is the CI gate)
make conformance   # normalise real corpus lines against the contract
```

All three run in a container — no host protobuf toolchain required. `buf` is
used when installed; otherwise the image's `grpcio-tools` provides protoc.

## CI and tests

The model in one line: **the `.proto` is the source of truth, `generated/` is
derived and committed, and CI verifies — it never produces.** No pipeline
generates anything a consumer reads.

Two gates, catching different failures:

| Gate | Command | Runs | Catches |
| :--- | :--- | :--- | :--- |
| drift | `make check` | `.github/workflows/schemas.yml`, on PRs and on pushes to `main` that touch `platform/schemas/**` | a `.proto` edited without regenerating |
| conformance | `make conformance` | locally, on demand | a contract that generates cleanly but does not fit the bytes the node actually writes |

Drift is a **provenance** check: it proves `generated/` still describes
`proto/`. Conformance is a **truth** check: it proves the contract describes
the data. Neither proves the third thing — that pyiceberg will accept the
resulting tables.

### What each gate actually does

- **`make check`** — `python tools/generate_schemas.py --check` compiles the
  protos to a descriptor set, regenerates into a **temporary directory**, and
  diffs that against the committed files. The only code path that writes
  `generated/` sits after the `--check` early return, so a passing run cannot
  modify the tree. Seconds, no corpus, no image.
- **`make conformance`** — `tools/check_corpus_conformance.py` takes real lines
  from a node hour-file (all three book-diff variants, the fills positional
  tuple, a nested order), runs them through the normaliser, and builds the
  Arrow tables from the **generated** schemas, asserting each table's schema
  equals the contract. This is the check that keeps the field census honest as
  the node's output changes.

### Tests around the contract

- **Generator invariants are enforced in code, not merely documented**: an
  event row missing any of the six identity fields is refused, and recursion
  beyond `MAX_ORDER_DEPTH` is reported rather than silently dropped.
- **The indexer's suite** (`services/hyperdata-indexer-hypercore`) mounts this
  contract and pins the closed variant sets — an unknown `raw_book_diff`
  variant, a non-list `order.children`, a non-string size must all raise. Its
  own CI is still a mockup.
- **The field list came from measurement**, not design: a census of 100k lines
  per table from the Jun-10 capture. Conformance is what keeps that census
  honest.

### Not covered yet

See [TODO](#todo) at the end of this file.

## Rules

1. **Edit the `.proto`, never `generated/`.** The generated tree is derived and
   committed; CI regenerates and fails on any difference. Hand-edits are
   invisible until the next `make generate`, and then they are lost.
2. **The contract describes storage rows, not wire lines.** The wire shape — the
   book-diff union and the fills positional tuple — belongs to the indexer's
   normaliser, which enforces the closed variant set. Neither draft-07 JSON
   Schema nor Arrow's JSON reader can express those two shapes: Arrow rejects a
   column that changes type mid-file, and draft-07 has no `prefixItems`.
3. **Every column is nullable.** The raw layer stays permissive so schema
   evolution never needs a column-relaxation rewrite. The identity columns
   (`block_number`, `block_time`, `local_time`, `log_index`, `source_file`,
   `ingest_ts`) are guaranteed by the generator instead, which refuses to emit
   an event row that is missing one.
4. **Decimals and categories stay strings.** `px`, `sz`, `orig_sz`, `new_sz`,
   `limit_px`, `trigger_px`, `closed_pnl`, `fee`, `start_position` are decimal
   strings, and `side`, `coin`, `status`, `order_type`, `tif`, `dir`,
   `trigger_condition`, `fee_token`, `diff_type` are string categories (9–11
   observed values, ~40% namespaced coins). Resolved in dbt, not at ingest.
5. **Timestamps are temporal.** `google.protobuf.Timestamp` becomes
   `timestamp[us, tz=UTC]`; nanoseconds on the wire are truncated to
   microseconds, which is Iceberg's precision and what `hours()`/`days()`
   partitioning needs.
6. **Recursion is bounded.** `Order.children` is recursive on the wire; the
   generator unrolls it to `MAX_ORDER_DEPTH` (3) and anything deeper is
   preserved at runtime in `children_overflow_json` — truncated, never dropped.
7. **Row identity is not a dedup key.** `(block_number, log_index)` orders
   events; idempotency is file-level, keyed `(path, sha256)`, because a reorg
   re-emits the same block numbers and there is no block hash in the L1 output to
   tell the two apart.

## Consumers

- `services/hyperdata-indexer-hypercore` — mounts `generated/arrow` at
  `/schemas/generated/arrow` (`SCHEMA_DIR`) and imports `hypercore_arrow.*`.
- `jobs/flink/parse-node-outputs` — generates its Java row classes from the same
  `.proto` with `protoc`, so the two writers cannot drift.

## Adding a table

1. Write `proto/hypercore/v1/<table>.proto` with a `<Table>Row` message carrying
   the six identity fields.
2. Register it in `EVENT_ROWS` (or `BLOCK_INFO`) in `tools/generate_schemas.py`.
3. Add its partition spec to `PARTITION_SPECS` in the indexer's `sink.py`.
4. `make generate`, then `make conformance` against real lines of the new table.

## TODO

What the two CI gates do not yet cover, hardest-value first.

- [ ] **`buf lint` and `buf breaking` in CI** — the highest-value addition.
      Nothing detects a *breaking* contract change (a dropped or retyped column)
      except conformance failing later, on new data, in the writer rather than in
      the review that introduced it.
- [ ] **Run conformance on every PR.** It needs the corpus today, and the dump
      is ~344 GB and not in the repo. Commit a small fixture set instead — a few
      hundred real-shape lines per table, covering all three book-diff variants,
      the fills tuple and a nested order — and the check runs anywhere.
- [ ] **Version and pin the contract.** Consumers track a branch, so there is no
      way to ask which contract produced a given set of tables. Needs a tag (or a
      `contract_version` recorded in table metadata) before the first breaking
      change, not after.
- [ ] **Integration test: a row reaches Iceberg.** Belongs in the indexer repo
      (already in its README TODO) — bring up the `warehouse` profile, ingest one
      hour-file, assert row counts and that a re-run is a no-op. It is what would
      catch a contract that generates cleanly and conforms to the corpus but that
      pyiceberg will not accept.
- [ ] **Decide whether drift should compare semantics.** It compares text, so
      reformatting the generator's output fails the gate for a contract that did
      not really change. The diff shows which; it cannot tell you it is harmless.
