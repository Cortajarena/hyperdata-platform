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
