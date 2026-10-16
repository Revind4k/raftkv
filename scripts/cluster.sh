#!/usr/bin/env bash
# Start a 3-node raftkv cluster on localhost:9001-9003. Ctrl-C stops all three.
# Data goes in ./data/<id>; delete it to start fresh.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/raftkv ./cmd/raftkv
PEERS=1=http://127.0.0.1:9001,2=http://127.0.0.1:9002,3=http://127.0.0.1:9003
trap 'kill $(jobs -p) 2>/dev/null' EXIT
for id in 1 2 3; do
  bin/raftkv -id "$id" -peers "$PEERS" -data "data/$id" &
done
wait
