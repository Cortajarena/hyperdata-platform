"""Generate the ingestion schemas from the .proto contract.

    .proto --(buf | protoc)--> FileDescriptorSet --(this)--> generated/

Two artifacts per table, both derived from ONE descriptor so they cannot drift:

    generated/arrow/hypercore_arrow/<table>.py   pa.schema([...]) module
    generated/json/<table>.json                 JSON Schema, draft-07

Both are committed (see ../README.md): consumers read or import them with no
protoc toolchain, and CI regenerates and fails on drift (`--check`).

Why an importable module for the Arrow side: pyarrow exposes no schema
to_json/from_json (verified on 25.0.1), so a JSON encoding would mean either a
bespoke format plus a bespoke loader, or opaque base64 IPC. Generated Python is
exact, diff-friendly, and needs no loader.

The contract is the STORAGE ROW, not the wire line. The wire shape — the
book-diff union and the fills positional tuple — belongs to the indexer's
normaliser, which enforces the closed variant set; neither draft-07 JSON Schema
nor Arrow's JSON reader can express those two shapes (see docs/ingestion.md).

Usage:
    python tools/generate_schemas.py [--proto-dir D] [--out-dir D] [--check]

    --check  generate to a temp dir and diff against the committed output;
             exit 1 on any difference. This is the CI drift gate.
"""

from __future__ import annotations

import argparse
import difflib
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field

from google.protobuf import descriptor_pb2

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Recursion bound for Order.children. Depth 2 is the deepest observed in the
# Jun-10 corpus (only 3 of 200,000 lines carry any child); 3 gives headroom.
# Deeper nodes are not dropped — the normaliser preserves them verbatim in the
# row's children_overflow_json column.
MAX_ORDER_DEPTH = 3

# Event rows must carry envelope + identity. Enforced here so a new table
# cannot quietly omit them. The proto file is named explicitly: deriving it from
# the message name ("RawBookDiffRow" -> "rawbookdiff") gets it wrong.
EVENT_ROWS = {
    "RawBookDiffRow": ("raw_book_diffs", "book_diffs.proto"),
    "OrderStatusRow": ("order_statuses", "order_statuses.proto"),
    "FillRow": ("fills", "fills.proto"),
}
BLOCK_INFO = ("BlockInfoRow", "block_info", "block_info.proto")

IDENTITY_FIELDS = (
    "block_number",
    "block_time",
    "local_time",
    "log_index",
    "source_file",
    "ingest_ts",
)

PROTO_SCALARS = {
    "TYPE_INT64": "int64",
    "TYPE_UINT64": "int64",
    "TYPE_INT32": "int32",
    "TYPE_UINT32": "int32",
    "TYPE_SINT32": "int32",
    "TYPE_SINT64": "int64",
    "TYPE_BOOL": "bool",
    "TYPE_STRING": "string",
    "TYPE_BYTES": "binary",
    "TYPE_DOUBLE": "float64",
    "TYPE_FLOAT": "float32",
}

JSON_SCALARS = {
    "int64": {"type": "integer"},
    "int32": {"type": "integer"},
    "bool": {"type": "boolean"},
    "string": {"type": "string"},
    "binary": {"type": "string", "contentEncoding": "base64"},
    "float64": {"type": "number"},
    "float32": {"type": "number"},
}

TIMESTAMP_JSON = {"type": "string", "format": "date-time"}
TIMESTAMP_ARROW = "timestamp[us, tz=UTC]"

JSON_SCHEMA_DRAFT = "http://json-schema.org/draft-07/schema#"


class ContractError(Exception):
    """The .proto contract breaks an invariant this generator enforces."""


# ---------------------------------------------------------------------------
# type model
# ---------------------------------------------------------------------------
@dataclass
class Column:
    name: str
    type: "Type"


@dataclass
class Type:
    kind: str                                  # scalar name | "struct" | "list"
    fields: list = field(default_factory=list)  # [(Column)] for struct
    item: object = None                          # Type for list


def arrow_type(t: Type) -> str:
    if t.kind == "struct":
        inner = ", ".join(f'"{c.name}": {arrow_type(c.type)}' for c in t.fields)
        return f"struct<{inner}>"
    if t.kind == "list":
        return f"list<{arrow_type(t.item)}>"
    return t.kind


def json_type(t: Type) -> dict:
    if t.kind == "struct":
        return {
            "type": "object",
            "properties": {c.name: json_type(c.type) for c in t.fields},
            "additionalProperties": False,
        }
    if t.kind == "list":
        return {"type": "array", "items": json_type(t.item)}
    if t.kind == "timestamp":
        return dict(TIMESTAMP_JSON)
    return dict(JSON_SCALARS[t.kind])



# ---------------------------------------------------------------------------
# descriptor -> types
# ---------------------------------------------------------------------------
def field_type(field, depth: int, stack: tuple,
               notes: list[str]) -> Type | None:
    """Type for one field, or None if the recursion bound stopped expansion.

    Returning None is how the bound applies: the field is dropped from the
    generated schema and the normaliser keeps those values in the row's
    *_overflow_json column instead, so nested data is never lost — only
    unqueryable without a JSON read. Every drop is reported.
    """
    kind = descriptor_pb2.FieldDescriptorProto.Type.Name(field.type)
    if field.type == field.TYPE_MESSAGE:
        name = field.message_type.name
        if name == "Timestamp":
            return Type("timestamp")
        if depth >= MAX_ORDER_DEPTH:
            notes.append(
                f"{field.containing_type.name}.{field.name} not expanded "
                f"(depth bound {MAX_ORDER_DEPTH}); preserved at runtime in "
                "an *_overflow_json column"
            )
            return None
        return Type("struct",
                    fields=message_columns(field.message_type, depth + 1, notes))
    scalar = PROTO_SCALARS.get(kind)
    if scalar is None:
        raise ContractError(f"unsupported field type: {kind}")
    return Type(scalar)


def message_columns(message, depth: int = 0, notes: list | None = None) -> list:
    notes = notes if notes is not None else []
    columns: list[Column] = []
    for field in message.fields:
        base = field_type(field, depth, (), notes)
        if base is None:                       # recursion bound; data is kept
            continue                           # in the overflow column instead
        if field.is_repeated:
            base = Type("list", item=base)
        columns.append(Column(_snake(field.name), base))
    return columns


def _snake(name: str) -> str:
    out: list[str] = []
    for i, ch in enumerate(name):
        if ch.isupper() and i and not name[i - 1].isupper():
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


# ---------------------------------------------------------------------------
# compile + render
# ---------------------------------------------------------------------------
def compile_protos(proto_dir: pathlib.Path, out: pathlib.Path) -> None:
    files = sorted(str(p.relative_to(proto_dir)) for p in proto_dir.rglob("*.proto"))
    if not files:
        raise ContractError(f"no .proto files under {proto_dir}")
    if shutil.which("buf"):
        cmd = ["buf", "build", "--as-file-descriptor-set", "-o", str(out),
               str(proto_dir)]
    else:
        cmd = _protoc() + [f"--proto_path={proto_dir}",
                           f"--descriptor_set_out={out}",
                           "--include_imports", *files]
    subprocess.run(cmd, check=True, capture_output=True)


def _protoc() -> list[str]:
    try:
        import grpc_tools.protoc  # noqa: F401
    except ImportError:
        raise ContractError(
            "no protoc: install `buf`, or `pip install grpcio-tools`"
        ) from None
    return [sys.executable, "-m", "grpc_tools.protoc"]


def load_pool(descriptor: pathlib.Path):
    """FileDescriptorSet -> DescriptorPool.

    A pool (not the raw FileDescriptorProtos) is what resolves type references,
    so `field.message_type` is populated. A fresh pool also keeps the bundled
    google/protobuf/timestamp.proto from colliding with the default one, which
    is why the descriptor set is compiled with --include_imports.
    """
    from google.protobuf import descriptor_pb2, descriptor_pool

    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(descriptor.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for f in fds.file:
        pool.Add(f)
    return pool


def find_message(pool, name: str):
    try:
        return pool.FindMessageTypeByName(f"hypercore.v1.{name}")
    except KeyError:
        raise ContractError(f"message hypercore.v1.{name} not found in protos") \
            from None


def build(pool) -> tuple[dict[str, dict[str, str]], list[str]]:
    """(table -> {arrow, json} sources, notes about anything not expanded)."""
    artifacts: dict[str, dict[str, str]] = {}
    notes: list[str] = []

    targets = [(msg, table, proto)
               for msg, (table, proto) in EVENT_ROWS.items()] + [BLOCK_INFO]
    for message_name, table, proto_file in targets:
        columns = message_columns(find_message(pool, message_name), notes=notes)

        if message_name in EVENT_ROWS:
            missing = [f for f in IDENTITY_FIELDS
                       if f not in {c.name for c in columns}]
            if missing:
                raise ContractError(
                    f"{message_name} is an event row but is missing identity "
                    f"field(s): {', '.join(missing)}"
                )

        artifacts[table] = {
            "arrow": _arrow_module(table, message_name, proto_file, columns),
            "json": _json_schema(table, message_name, columns),
        }
    return artifacts, notes


INDENT = "    "


def _arrow_lines(t: Type, indent: str) -> list[str]:
    """Render a pyarrow type, wrapped so no generated line runs long.

    Nested structs are what make these schemas big (order_statuses unrolls
    Order.children three deep); unwrapped, that is a 1,800-character line that
    nobody can review in a diff.
    """
    pad = indent + INDENT
    if t.kind == "struct":
        lines = [f"{indent}pa.struct(["]
        for col in t.fields:
            lines += _field_lines(col, pad)
        lines.append(f"{indent}])")
        return lines
    if t.kind == "list":
        return [f"{indent}pa.list_("] + _arrow_lines(t.item, pad) + \
            [f"{indent})"]
    if t.kind == "timestamp":
        return [f'{indent}pa.timestamp("us", tz="UTC")']
    if t.kind == "binary":
        return [f"{indent}pa.binary()"]
    if t.kind == "bool":
        return [f"{indent}pa.bool_()"]      # pyarrow has no pa.bool()
    return [f"{indent}pa.{t.kind}()"]


def _field_lines(col: Column, indent: str) -> list[str]:
    pad = indent + INDENT
    body = _arrow_lines(col.type, pad)
    one_line = f'{indent}pa.field("{col.name}", {body[0].strip()}),'
    if len(one_line) <= 88 and len(body) == 1:
        return [one_line]
    return ([f"{indent}pa.field(",
             f'{pad}"{col.name}",', *body, f"{indent}),"])


def _arrow_module(table: str, message: str, proto_file: str,
                  columns: list[Column]) -> str:
    lines = [
        '"""Generated by tools/generate_schemas.py — do not edit.',
        "",
        f"Source:    proto/hypercore/v1/{proto_file}",
        f"Message:   hypercore.v1.{message}",
        f"Table:     hypercore.{table}",
        "",
        "Every column is nullable, deliberately: the raw layer stays permissive so",
        "Iceberg schema evolution never needs a column-relaxation rewrite. The",
        "identity columns are guaranteed by the contract instead — see",
        "tools/generate_schemas.py IDENTITY_FIELDS.",
        '"""',
        "",
        "import pyarrow as pa",
        "",
        "SCHEMA = pa.schema([",
    ]
    for col in columns:
        lines += _field_lines(col, "    ")
    lines += ["])", ""]
    return "\n".join(lines)


def _json_schema(table: str, message: str, columns: list[Column]) -> str:
    schema = {
        "$schema": JSON_SCHEMA_DRAFT,
        "$id": f"https://github.com/Cortajarena/hyperdata-platform/"
               f"blob/main/platform/schemas/generated/json/{table}.json",
        "title": f"hypercore.{table}",
        "description": f"Storage row contract for hypercore.{table} "
                       f"(from hypercore.v1.{message}).",
        "type": "object",
        "additionalProperties": False,
        "properties": {c.name: json_type(c.type) for c in columns},
    }
    return json.dumps(schema, indent=2) + "\n"



def write(artifacts: dict[str, dict[str, str]], out_dir: pathlib.Path) -> None:
    arrow_dir = out_dir / "arrow" / "hypercore_arrow"
    json_dir = out_dir / "json"
    arrow_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    (arrow_dir / "__init__.py").write_text(
        '"""Generated Arrow schemas — see tools/generate_schemas.py."""\n'
    )
    for table, files in artifacts.items():
        (arrow_dir / f"{table}.py").write_text(files["arrow"])
        (json_dir / f"{table}.json").write_text(files["json"])


def check(artifacts: dict[str, dict[str, str]], out_dir: pathlib.Path) -> int:
    """Diff freshly generated output against what is committed."""
    drifted: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        fresh = pathlib.Path(tmp)
        write(artifacts, fresh)
        committed = {
            p.relative_to(out_dir): p.read_text()
            for p in sorted(out_dir.rglob("*"))
            if p.is_file() and "__pycache__" not in p.parts
        }
        produced = {
            p.relative_to(fresh): p.read_text()
            for p in sorted(fresh.rglob("*"))
            if p.is_file() and "__pycache__" not in p.parts
        }
    for path in sorted(set(committed) | set(produced)):
        old = committed.get(path, "")
        new = produced.get(path, "")
        if old == new:
            continue
        drifted.append(str(path))
        diff = difflib.unified_diff(old.splitlines(), new.splitlines(),
                                    fromfile=f"committed/{path}",
                                    tofile=f"generated/{path}", lineterm="")
        print("\n".join(diff))
    if drifted:
        print(f"\n{len(drifted)} artifact(s) out of date: {', '.join(drifted)}")
        print("run `make schemas` and commit the result")
        return 1
    print(f"schemas up to date ({len(produced)} artifacts)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="generate ingestion schemas")
    parser.add_argument("--proto-dir", type=pathlib.Path,
                        default=ROOT / "proto")
    parser.add_argument("--out-dir", type=pathlib.Path,
                        default=ROOT / "generated")
    parser.add_argument("--check", action="store_true",
                        help="exit 1 if committed output has drifted")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        descriptor = pathlib.Path(tmp) / "descriptors.binpb"
        compile_protos(args.proto_dir, descriptor)
        artifacts, notes = build(load_pool(descriptor))
        for note in notes:
            print(f"note: {note}")
        if args.check:
            return check(artifacts, args.out_dir)
        write(artifacts, args.out_dir)
    for table in sorted(artifacts):
        print(f"wrote {table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
