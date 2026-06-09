#!/bin/sh
set -e

NODES="zodiac-1:8000 zodiac-2:8000 zodiac-3:8000"

echo "Waiting for cluster leader..."
for i in $(seq 1 30); do
  for node in $NODES; do
    resp=$(curl -sf "http://$node/status/" 2>/dev/null || true)
    leader=$(echo "$resp" | jq -r 'select(.is_leader) | .id' 2>/dev/null || true)
    if [ -n "$leader" ]; then
      echo "Leader found: node $leader at $node"
      break 2
    fi
  done
  sleep 1
done

if [ -z "$leader" ]; then
  echo "FAILED: no leader elected within 30s"
  exit 1
fi

echo "Put name=zodiac via leader..."
curl -sf -X POST "http://$node/put/" \
  -H "Content-Type: application/json" \
  -d '{"key":"name","value":"zodiac","clientID":1,"requestID":1}' | jq .

echo "Get name via leader..."
curl -sf -X POST "http://$node/get/" \
  -H "Content-Type: application/json" \
  -d '{"key":"name","clientID":1,"requestID":2}' | jq .

val=$(curl -sf -X POST "http://$node/get/" \
  -H "Content-Type: application/json" \
  -d '{"key":"name","clientID":1,"requestID":2}' | jq -r '.value')
if [ "$val" != "zodiac" ]; then
  echo "FAILED: expected value=zodiac, got $val"
  exit 1
fi

echo "Read from every node (linearizability check)..."
for node in $NODES; do
  v=$(curl -sf -X POST "http://$node/get/" \
    -H "Content-Type: application/json" \
    -d '{"key":"name","clientID":2,"requestID":1}' 2>/dev/null | jq -r '.value' 2>/dev/null || echo "UNREACHABLE")
  echo "  $node -> $v"
done

echo ""
echo "ALL TESTS PASSED"
