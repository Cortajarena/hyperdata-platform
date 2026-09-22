#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Idempotent Kafka topic bootstrap from topics.yaml.
#
# Local parity for the future K8s path: locally this runs as the `kafka-init`
# compose service; in K8s the same contract is applied as a one-shot Job (or
# rendered to Strimzi KafkaTopic CRs from the same topics.yaml).
#
# Safe to re-run: `--create --if-not-exists` makes it a no-op once topics exist.
# Parses with awk on purpose — the Kafka image ships no python/yq.
# ---------------------------------------------------------------------------
set -euo pipefail

TOPICS_FILE="${TOPICS_FILE:-/topics.yaml}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

# topics.yaml -> one line per topic: "name partitions replication retention.ms"
parse_topics() {
  awk '
    /^[[:space:]]*#/ { next }
    /^defaults:/     { sec="d"; next }
    /^topics:/       { sec="t"; next }
    sec=="d" && NF   { k=$1; sub(/:$/,"",k); def[k]=$2; next }
    sec=="t" && /- name:/ { flush(); n=$3; p=r=t=""; next }
    sec=="t" && NF   { k=$1; sub(/:$/,"",k); v=$2;
                       if(k=="partitions")p=v; else if(k=="replication")r=v;
                       else if(k=="retention.ms")t=v; next }
    END { flush() }
    function flush() {
      if (n!="") printf "%s %s %s %s\n", n, p,
        (r!=""?r:def["replication"]), (t!=""?t:def["retention.ms"])
    }
  ' "$TOPICS_FILE"
}

echo ">> ensuring topics from ${TOPICS_FILE}"
while read -r name partitions replication retention_ms; do
  [[ -z "$name" ]] && continue
  echo "   ${name} (partitions=${partitions} replication=${replication} retention.ms=${retention_ms})"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config "retention.ms=${retention_ms}"
done < <(parse_topics)

echo ">> topics now:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list