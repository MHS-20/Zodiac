#!/bin/sh
set -e

cat > /config.json <<EOF
{
  "node_id": ${NODE_ID},
  "http_port": ${HTTP_PORT},
  "data_dir": "${DATA_DIR}",
  "initial_cluster": ${CLUSTER}
}
EOF

exec /zodiac /config.json
