# ClickHouse (platform system, not a layer service)

Analytical serving store for the **derived data tier** — TOPIC of contracts
here is the runtime definition only. Table/view/BI-model ownership lives in
`jobs/` (dbt-clickhouse models, or the Phase-5 job that derives book-state
streams — see `docs/ingestion.md` appendix). Cloud provisioning belongs in
`infrastructure/terraform` later (or a BYOL ClickHouse Cloud tenant, matching
the root README's multi-BYOL-warehouse promise).

## Quickstart (single node, no other slice needed)

```bash
cd platform/clickhouse
docker compose --profile serving up -d
```

HTTP interface — one-shot query, no client binary required (dev credentials,
from `compose.yaml`):

```bash
curl 'http://localhost:8123/ping'                                        # -> Ok. (no auth: ping is open)
curl --user 'hyperdata:hyperdata-dev' 'http://localhost:8123/?query=SELECT 1'
```

In-network / native protocol:

- clickhouse-client inside the compose network: `clickhouse-client` on `clickhouse:9000`
- host-side native port is `9100` (host `9000` is already MinIO's S3 API)

Stop / wipe (profile + named volume):

```bash
docker compose --profile serving down        # keep data
docker compose --profile serving down -v     # wipe (also resets the users)
```

## Ports (host side)

| Port | Protocol | Note |
|---|---|---|
| 8123 | HTTP | free — the canonical CH HTTP port |
| 9100 | native | remapped: host `9000` is already MinIO's S3 API; in-network stays `clickhouse:9000` |

## Config layout

| Path | Mounted to | Contents |
|---|---|---|
| `config/users.xml` | `/etc/clickhouse-server/users.d/hyperdata.xml` | user policy only (read-only analytics pending); file-point mount so the image's entrypoint still manages its own boot-time files |

Note: the upstream image **hardens the login entrypoint** — without
`CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` env it disables networked default
user access and refuses to start. Dev credentials live as plain env in
`compose.yaml` (same pattern as MinIO's `hyperdata-dev`).

Engine-server settings (not user policy) will live in *config-server/*,
mounted over `config.d` — kept separate so serving-user policy never tangles
with engine tuning. By the cluster stage: no in-compose `clickhouse-data` —
moves to `infrastructure/kubernetes/` manifests.
