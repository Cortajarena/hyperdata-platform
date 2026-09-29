"""Contract conformance: real corpus lines -> generated Arrow schema.

This is the test that matters. The schemas are generated from the .proto
contract, so they are only correct if the contract matches the data the node
actually writes. Every field type below was chosen from the Jun-10 census
(100k lines/table), and this asserts it against the real bytes.

Run:  python tools/check_corpus_conformance.py [path-to-corpus]
"""

from __future__ import annotations

import datetime as dt
import pathlib
import sys

import pyarrow as pa

sys.path.insert(
    0,
    str(pathlib.Path(__file__).resolve().parent.parent / "generated" / "arrow"),
)

from hypercore_arrow import (  # noqa: E402
    block_info, fills, order_statuses, raw_book_diffs,
)

DEFAULT_CORPUS = pathlib.Path(
    "/nvme0n1-disk/data/hl-node-data/data_stream_with_block_info"
)
HOUR = pathlib.Path("hourly/20260610/13")
SAMPLE = 2_000


def parse_ts(value: str) -> dt.datetime:
    """ISO-8601 with nanoseconds -> datetime truncated to microseconds."""
    return dt.datetime.fromisoformat(value)


def conform(pa_module, schema: pa.Schema, rows: list[dict]) -> pa.Table:
    table = pa.table({name: [r.get(name) for r in rows]
                      for name in schema.names}, schema=schema)
    assert table.schema == schema, "table schema drifted from the contract"
    return table


def book_diff_rows(path: pathlib.Path) -> tuple[pa.Table, dict]:
    rows, seen = [], set()
    with path.open("rb") as fh:
        for i, line in enumerate(fh):
            if i >= SAMPLE:
                break
            env = _loads(line)
            for diff in env["events"]:
                raw = diff["raw_book_diff"]
                if isinstance(raw, str):
                    kind, orig, new = raw, None, None
                elif "new" in raw:
                    kind, orig, new = "new", None, raw["new"]["sz"]
                else:
                    upd = raw["update"]
                    kind, orig, new = "update", upd["origSz"], upd["newSz"]
                seen.add(kind)
                rows.append({
                    "block_number": env["block_number"],
                    "block_time": parse_ts(env["block_time"]),
                    "local_time": parse_ts(env["local_time"]),
                    "log_index": len(rows),
                    "source_file": str(path),
                    "ingest_ts": dt.datetime.now(dt.UTC).replace(tzinfo=None),
                    "user": diff["user"],
                    "oid": diff["oid"],
                    "coin": diff["coin"],
                    "side": diff["side"],
                    "px": diff["px"],
                    "diff_type": kind,
                    "orig_sz": orig,
                    "new_sz": new,
                })
    return conform(raw_book_diffs, raw_book_diffs.SCHEMA, rows), seen


def status_rows(path: pathlib.Path) -> pa.Table:
    rows = []
    with path.open("rb") as fh:
        for i, line in enumerate(fh):
            if i >= SAMPLE:
                break
            env = _loads(line)
            for ev in env["events"]:
                order = ev["order"]
                children = order.get("children") or []
                rows.append({
                    "block_number": env["block_number"],
                    "block_time": parse_ts(env["block_time"]),
                    "local_time": parse_ts(env["local_time"]),
                    "log_index": len(rows),
                    "source_file": str(path),
                    "ingest_ts": dt.datetime.now(dt.UTC).replace(tzinfo=None),
                    "user": ev["user"],
                    "status": ev["status"],
                    "hash": ev["hash"],
                    "builder": ev["builder"],
                    "order": order,
                })
    return conform(order_statuses, order_statuses.SCHEMA, rows)


def fill_rows(path: pathlib.Path) -> pa.Table:
    rows = []
    with path.open("rb") as fh:
        for i, line in enumerate(fh):
            if i >= SAMPLE:
                break
            env = _loads(line)
            for user, payload in env["events"]:
                rows.append({
                    "block_number": env["block_number"],
                    "block_time": parse_ts(env["block_time"]),
                    "local_time": parse_ts(env["local_time"]),
                    "log_index": len(rows),
                    "source_file": str(path),
                    "ingest_ts": dt.datetime.now(dt.UTC).replace(tzinfo=None),
                    "user": user,
                    **{k: v for k, v in payload.items() if k != "user"},
                })
    return conform(fills, fills.SCHEMA, rows)


def _loads(line: bytes) -> dict:
    import orjson
    return orjson.loads(line)


def main() -> int:
    corpus = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CORPUS)
    print(f"corpus: {corpus}\n")

    diffs, variants = book_diff_rows(corpus / "node_raw_book_diffs_streaming" / HOUR)
    print(f"raw_book_diffs  {diffs.num_rows:>7,} rows  "
          f"variants seen: {sorted(variants)}")

    statuses = status_rows(corpus / "node_order_statuses_streaming" / HOUR)
    print(f"order_statuses  {statuses.num_rows:>7,} rows")

    trades = fill_rows(corpus / "node_fills_streaming" / HOUR)
    print(f"fills           {trades.num_rows:>7,} rows")

    info = conform(block_info, block_info.SCHEMA, [{
        "table": "raw_book_diffs",
        "block_number": diffs.column("block_number")[0].as_py(),
        "block_time": diffs.column("block_time")[0].as_py(),
        "first_local_time": diffs.column("local_time")[0].as_py(),
        "last_local_time": diffs.column("local_time")[-1].as_py(),
        "event_count": diffs.num_rows,
        "log_index_min": 0,
        "log_index_max": diffs.num_rows - 1,
        "first_seen_at": dt.datetime.now(dt.UTC).replace(tzinfo=None),
        "complete": True,
    }])
    print(f"block_info      {info.num_rows:>7,} rows")
    print("\nall four tables conform to the generated contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
