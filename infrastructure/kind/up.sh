#!/usr/bin/env bash
# Create the local HyperData dev cluster (idempotent — safe to re-run).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_NAME=hyperdata

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "cluster '$CLUSTER_NAME' already exists — nothing to do"
else
  kind create cluster --config "${SCRIPT_DIR}/kind-config.yaml"
fi

echo
echo "verify:  kubectl get nodes"
echo "delete:  kind delete cluster --name $CLUSTER_NAME"
