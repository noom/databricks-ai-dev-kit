#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-noom-databricks-dev}"
METHOD="${2:-tools/list}"
SQL="${3:-SELECT COUNT(*) AS row_count FROM common_data.dim_date}"
MCP_URL="https://mcp-noom-dev-638571477831686.aws.databricksapps.com/mcp"

TOKEN=$(databricks auth token -p "$PROFILE" --output json | jq -r .access_token)

PAYLOAD=$(python3 -c "
import json, sys
method, sql = sys.argv[1], sys.argv[2]
if method == 'initialize':
    body = {'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'test','version':'1.0'}}}
elif method == 'tools/list':
    body = {'jsonrpc':'2.0','id':2,'method':'tools/list','params':{}}
elif method == 'execute_sql':
    body = {'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':'execute_sql','arguments':{'sql_query':sql}}}
elif method == 'list_warehouses':
    body = {'jsonrpc':'2.0','id':4,'method':'tools/call','params':{'name':'manage_warehouse','arguments':{'action':'list'}}}
else:
    print(f'Unknown method: {method}', file=sys.stderr)
    sys.exit(1)
print(json.dumps(body))
" "$METHOD" "$SQL")

echo ">>> Request: $PAYLOAD"
echo ""

RESPONSE=$(curl -s -w "\n\nHTTP_STATUS:%{http_code}" -X POST "$MCP_URL" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer $TOKEN" \
  --data "$PAYLOAD")

HTTP_STATUS=$(echo "$RESPONSE" | grep "HTTP_STATUS:" | cut -d: -f2)
# Extract JSON from SSE data: lines format
BODY=$(echo "$RESPONSE" | grep "^data:" | sed 's/^data: //' | head -1)

echo "<<< HTTP Status: $HTTP_STATUS"
echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "<<< Raw: $BODY"
