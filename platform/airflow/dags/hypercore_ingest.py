"""Periodic ingestion of HyperCore node output files into Iceberg.

MOCKUP — the shape is real, the wiring is not finished. Read this before
trusting anything below.

What this DAG is for
--------------------
The node writes hour-files continuously; the indexer turns sealed hour-files
into committed `hypercore.*` tables. Something has to run that indexer on a
schedule, and that something is orchestration, not the compose stack. This DAG
is that "something", deliberately kept to one task so the first run tells us
whether the shape is right.

    every 10 min ──► docker run hyperdata-indexer run
                        │
                        ├─ sealed hour-files it has not committed  → Iceberg
                        └─ files already in the ledger           → skipped

Why a BashOperator running `docker run`, and not the indexer imported here
------------------------------------------------------------------------------
Three reasons, in order of importance:

1. Version independence. The DAG and the indexer are separate repos with
   separate release cycles. Running the published image means a DAG change
   cannot break ingestion, and an ingestion change needs no Airflow redeploy.
2. One dependency set. The indexer pins pyarrow and pyiceberg exactly. Adding
   them to the Airflow image would put a second, conflicting Arrow on the
   orchestration box — the classic way to make a DAG "work locally" and break
   in CI.
3. The production target is Kubernetes, where a DAG cannot import anything
   from the node anyway. The commented KubernetesPodOperator at the bottom is
   the shape this becomes; a BashOperator wrapping `docker run` is the local
   equivalent, so the DAG logic does not change when the executor target does.

What is mocked / unfinished
---------------------------
* NODE_DIR points at the platform's `node-outputs` volume, but the real node
  bind-mounts `${DATA_DIR}/hl-node-data` -> /home/hluser/hl (see
  services/hyperdata-node). Until the two agree, the DAG finds no files. That
  reconciliation is the next branch: the node's replay mode plus the poll
  service reading the node's shared output volume.
* No warehouse dependency. The `warehouse` profile (iceberg-catalog + minio)
  is a separate compose profile; this DAG assumes both are up. There is no
  sensor or wait-for-catalog step yet.
* `image` is built on demand (idempotent, ~1s when cached) rather than being
  published to a registry.
* Schedule and concurrency are guesses: 10 minutes, one active run. Real
  numbers depend on the seal cadence and how long a backfill window takes.
"""

from __future__ import annotations

import logging

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# One run per table: the three streams have different rates and different
# files, and a run is the unit of recovery (the ledger is per table).
TABLES = ["raw_book_diffs", "order_statuses", "fills"]

# Defaults only. The bash commands below are Jinja-templated, so anything set
# here can be overridden per run from the Airflow UI without editing the DAG.
DEFAULT_PARAMS = {
    "table": "fills",                 # smallest stream; the others behave the same
    "image": "hyperdata-indexer:latest",
    # Host paths, because `docker run` resolves volumes on the host, not in
    # whatever container Airflow itself is in.
    "node_dir": "/data/hl-node-data/data",
    "schemas": (
        "/workspaces/hyperdata-platform/platform/schemas/generated/arrow"
    ),
    "context": (
        "/workspaces/hyperdata-platform/services/hyperdata-indexer-hypercore"
    ),
    "state_volume": "hypercore-indexer-state",
    "finalized_only": "true",         # skip the current, still-growing hour
    "buffer_bytes": str(256 * 1024 * 1024),
}

BUILD_COMMAND = """
set -euo pipefail
# Idempotent: a no-op (~1s) when the image is cached. A DAG edit must not
# require an image rebuild, and an image change must not require a DAG edit.
docker build -t {{ params.image }} {{ params.context }}
"""

# `docker run` — the actual ingestion, as one explicit command.
#
# Volumes, in order of how they went wrong before:
#   /schemas/generated/arrow  the row contract, generated from
#                             platform/schemas and mounted read-only. Without
#                             it the indexer refuses to start rather than
#                             guess a schema.
#   <node_dir>                the node's output tree, read-only.
#   state volume              the ingest ledger and the dead-letter queue. The
#                             ledger is what makes a re-run a no-op, so it
#                             must outlive the container.
#
# Warehouse settings come from the Airflow container's environment ($VAR) and
# are expanded by bash, not templated here: they are deployment facts, not
# per-run choices.
RUN_COMMAND = """
set -euo pipefail
docker run --rm \\
  --name hypercore-indexer-{{ params.table }} \\
  -e TABLE={{ params.table }} \\
  -e NODE_DIR=/node \\
  -e SCHEMA_DIR=/schemas/generated/arrow \\
  -e STATE_DIR=/state \\
  -e CATALOG_URI="$HYPERCORE_CATALOG_URI" \\
  -e WAREHOUSE="$HYPERCORE_WAREHOUSE" \\
  -e S3_ENDPOINT="$HYPERCORE_S3_ENDPOINT" \\
  -e S3_ACCESS_KEY_ID="$HYPERCORE_MINIO_USER" \\
  -e S3_SECRET_ACCESS_KEY="$HYPERCORE_MINIO_PASSWORD" \\
  -e BUFFER_BYTES={{ params.buffer_bytes }} \\
  -e FINALIZED_ONLY={{ params.finalized_only }} \\
  -v {{ params.schemas }}:/schemas/generated/arrow:ro \\
  -v {{ params.node_dir }}:/node:ro \\
  -v {{ params.state_volume }}:/state \\
  {{ params.image }} run
"""


with DAG(
    dag_id="hypercore_ingest",
    description="Ingest sealed HyperCore node hour-files into Iceberg",
    # Every 10 minutes: a guess. The real cadence should follow the seal
    # interval and how long a backfill window takes — not this number.
    schedule="@every 10 minutes",
    # pendulum, not airflow.utils.dates: that module is gone in Airflow 2.9.
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # One at a time: two runs would race on the same ledger and the same
    # hour-files. The indexer is idempotent, but not concurrency-safe.
    max_active_runs=1,
    default_args={"retries": 0, "depends_on_past": False},
    params=DEFAULT_PARAMS,
    tags=["ingestion", "hypercore", "iceberg"],
) as dag:
    build = BashOperator(
        task_id="build_indexer_image",
        bash_command=BUILD_COMMAND,
    )

    ingest = BashOperator(
        task_id="ingest_hour_files",
        bash_command=RUN_COMMAND,
        # The ledger makes a retry cheap, so this is the task that would get
        # retries in production — one transient catalog error should not need a
        # human. Left at 0 here because the wiring above is still mocked.
        retries=0,
    )

    build >> ingest


# ---------------------------------------------------------------------------
# Production shape: the same DAG, targeting Kubernetes instead of the local
# docker socket. Kept here as the reference for the migration — the task
# boundary, params and retry policy carry over unchanged.
#
#     from airflow.providers.cncf.kubernetes.operators.pod import (
#         KubernetesPodOperator,
#     )
#
#     KubernetesPodOperator(
#         task_id="ingest_hour_files",
#         name="hypercore-indexer-{{ params.table }}",
#         image="{{ params.image }}",
#         cmds=["run"],
#         env_vars={
#             "TABLE": "{{ params.table }}",
#             "CATALOG_URI": "{{ var.value.catalog_uri }}",
#             "WAREHOUSE": "{{ var.value.warehouse }}",
#         },
#         volume_mounts=[
#             {"name": "schemas", "mount_path": "/schemas/generated/arrow",
#              "read_only": True},
#             {"name": "node-outputs", "mount_path": "/node", "read_only": True},
#             {"name": "state", "mount_path": "/state"},
#         ],
#         volumes=[
#             {"name": "schemas", "persistent_volume_claim": {"claim_name":
#              "hyperdata-schemas"}},
#             {"name": "node-outputs", "persistent_volume_claim": {"claim_name":
#              "node-outputs"}},
#             {"name": "state", "empty_dir": {}},
#         ],
#         # The ledger must outlive the pod, so `state` becomes a PVC in the
#         # real deployment — an empty_dir would re-ingest every file on retry.
#     )
