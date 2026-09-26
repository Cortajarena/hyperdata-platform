# Services

Deployable, long-running processes owned by this repo: the Hyperliquid node, its
Kafka sidecar, cluster-facing pieces. (One-shot / transformation workloads live in
`jobs/`, platform systems in `platform/`.)

# Upgrading the Node: Deployment & Cutover Playbook

How a new version of the Hyperliquid node (and its producer) ships to Kubernetes
without losing or reordering data.

## What's on each side of the fence

| Component | State | Restart cost |
|---|---|---|
| Hyperliquid node | Local RocksDB state, big | **Hours** — no public state snapshots; new major version = fresh volume + data-bundle download + replay to head |
| Kafka | Passive data hub — no migration on its side (offsets live with producers/consumers) | ~none |
| Sidecar (node → Kafka) | "last acked offset" bookkeeping + publish mode | Must resume exactly, or we duplicate/reorder |

Kafka decouples production from consumption: if the node stops for 30 minutes,
consumers don't fail — lag spikes, then drains. **No scenario below loses data** as
long as (a) the sidecar resumes from a durable offset and (b) consumers are
idempotent on `(block height, row id)`. Those two invariants are load-bearing;
everything else is comfort.

## The one thing not to do

**Two live node versions publishing the same topic concurrently.** That's
interleaved, out-of-order events and duplicates at the seam — worse than any
downtime. Also: don't carry v1 node state into v2 (format-locked disposable
volume), and don't infill state from third-party snapshot drops — only the
official data bundle.

## Three choreographies

**A) In-place restart + replay gap-fill** *(start here for v0.1)*
Restart the node. Catch-up takes hours, the stream goes stale by that much, then
the replay tool (replay-as-tap) backfills the gap into Kafka. No loss, worst
latency story, cheapest to build.

**B) Catch-up-then-cutover (blue/green)** *(the real design)*
1. Deploy node-v2 **shadowed**: sidecar `mode: shadow` → writes to a `*-candidate`
   topic (or is muted) while it downloads the data bundle and replays to head.
   v1 keeps serving the live topic; nothing in the live path is affected.
2. When v2 lag ≈ 0, pick barrier block height B.
3. Cutover: v1 sidecar stops cleanly at B, v2 sidecar goes live from B+1 on the
   real topic, and a **watermark row** (`{v1_last: B, v2_first: B+1}`) is written
   to mark the seam.
4. Seam check: any block between v1's tail and v2's head is patched by the replay
   tool. Seam should be seconds, not hours.
5. Decommission v1 (keep its PVC a day or two, then delete).
Rollback = flip the mode label on v1's sidecar back to `live`. The node binaries
are never touched at cutover — the sidecar is the atomic switch.

**C) RollingUpdate** — only valid when the new version keeps the on-disk format
(restart resync is minutes). Check HL release notes; don't assume.

## Kubernetes / Helm mechanics

- Node runs as a **StatefulSet** (`charts/hyperdata-node`), one replica, one PVC
  **per major version** (never remount an old state dir under a new binary).
- Blue/green = **two parallel Helm releases of the same chart** (`helm install
  node-v1 …`, `helm install node-v2 …`), not a Deployment rollout strategy —
  RollingUpdate would fold catch-up latency into the live path.
- The only stateful knob is the sidecar publish **`mode: live | muted | shadow`**
  (a Helm value) plus the **topic** and durable offset. Cutover becomes `helm
  upgrade` + a short one-off **Job** that (a) stalls v1 at barrier B, (b) fires
  v2 at B+1, (c) writes the watermark, (d) runs the seam check.
- Head-ness gate before cutover is just metrics: `chain_head - published_height`
  ≈ 0 on v2's sidecar.

## Versioning the Kafka contract

- Payload shape changes: evolve additively (new fields only), or version the
  topic (`...v2`). Breaking change ⇒ migrate onto a *new topic* with a watermark
  seam, not in-place.
- Deploy dual-independent consumers first when formats change.
- **Watermark/barrier record is part of the topic contract from day one** — even
  if cutover tooling doesn't exist yet.

## What to build now vs later

Now (cheap, load-bearing): sidecar offset-resume + mode knobs; idempotent
consumers as an acceptance criterion in Flink tasks; watermark record schema.
Later: the parallel-release blue/green machinery — it's just Helm semantics once
the above exists.

Parking-lot note: ClickHouse and derived materializations are disposable
projections (see `platform/README.md` contract) — rebuild, never migrate.
Flink jobs upgrade via savepoint → deploy → resume.
