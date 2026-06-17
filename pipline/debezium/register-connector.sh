set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo ">> Registering connector at ${CONNECT_URL} ..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"${HERE}/connector-config.json" \
  "${CONNECT_URL}/connectors" | (python3 -m json.tool 2>/dev/null || cat) || true

echo
echo ">> Connector status:"
curl -s "${CONNECT_URL}/connectors/ecommerce-source-connector/status" \
  | (python3 -m json.tool 2>/dev/null || cat) || true
echo
