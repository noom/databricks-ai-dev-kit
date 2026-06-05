#!/bin/bash
# Deploy the Noom MCP server to Databricks Apps.
#
# Usage:
#   bash scripts/deploy.sh <app-name> --env ENV [--profile PROFILE] [--warehouse-id ID]
#
# Environments (--env):
#   dev   profile=dev,  host=noom-dev.cloud.databricks.com,  warehouse=12ce469e5394ac8b
#   prod  profile=prod, host=noom-prod.cloud.databricks.com, warehouse=575c0a43969584a4
#
# --profile and --warehouse-id override the env defaults when provided.
#
# Example:
#   bash scripts/deploy.sh mcp-noom-dev --env dev
#   bash scripts/deploy.sh mcp-noom-prod --env prod --warehouse-id <prod-warehouse-id>

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_DIR")"

APP_NAME=""
ENV=""
PROFILE=""
WAREHOUSE_ID=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --env) ENV="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --warehouse-id) WAREHOUSE_ID="$2"; shift 2 ;;
    -*)  echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    *)
      if [ -z "$APP_NAME" ]; then APP_NAME="$1"; else echo -e "${RED}Unexpected arg: $1${NC}"; exit 1; fi
      shift ;;
  esac
done

if [ -z "$APP_NAME" ]; then
  echo -e "${RED}Error: app name required.${NC}"
  echo "Usage: bash scripts/deploy.sh <app-name> --env ENV [--profile PROFILE] [--warehouse-id ID]"
  exit 1
fi

if [ -z "$ENV" ]; then
  echo -e "${RED}Error: --env is required (dev or prod).${NC}"
  echo "Usage: bash scripts/deploy.sh <app-name> --env ENV [--profile PROFILE] [--warehouse-id ID]"
  exit 1
fi

# Apply env defaults, then let explicit flags override.
case "$ENV" in
  dev)
    PROFILE="${PROFILE:-dev}"
    WAREHOUSE_ID="${WAREHOUSE_ID:-12ce469e5394ac8b}"
    ;;
  prod)
    PROFILE="${PROFILE:-prod}"
    WAREHOUSE_ID="${WAREHOUSE_ID:-575c0a43969584a4}"
    ;;
  *)
    echo -e "${RED}Error: unknown env '${ENV}'. Valid values: dev, prod.${NC}"
    exit 1
    ;;
esac

CLI_ARGS="--profile $PROFILE"
STAGING_DIR="/tmp/${APP_NAME}-deploy"

echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Noom MCP Server — Databricks Apps Deploy   ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  App:       ${GREEN}${APP_NAME}${NC}"
echo -e "  Profile:   ${PROFILE}"
echo -e "  Warehouse: ${WAREHOUSE_ID}"
echo ""

# ── Step 1: Auth check ──────────────────────────────────────────────────────
echo -e "${YELLOW}[1/5] Checking auth...${NC}"
if ! databricks auth describe $CLI_ARGS &>/dev/null; then
  echo -e "${RED}Not authenticated. Run: databricks auth login --profile ${PROFILE}${NC}"; exit 1
fi

WORKSPACE_HOST=$(databricks auth describe $CLI_ARGS 2>/dev/null | grep "^Host:" | awk '{print $2}')
CURRENT_USER=$(databricks current-user me $CLI_ARGS --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))")

echo -e "  Workspace: ${WORKSPACE_HOST}"
echo -e "  User:      ${CURRENT_USER}"

WORKSPACE_PATH="/Workspace/Users/${CURRENT_USER}/apps/${APP_NAME}"
echo -e "  Deploy to: ${WORKSPACE_PATH}"
echo ""

# ── Step 2: Staging ─────────────────────────────────────────────────────────
echo -e "${YELLOW}[2/5] Staging deployment package...${NC}"
rm -rf "$STAGING_DIR" && mkdir -p "$STAGING_DIR"

# Noom's customization and hosting layers
cp -r "$PROJECT_DIR/customization" "$STAGING_DIR/"
cp -r "$PROJECT_DIR/hosting" "$STAGING_DIR/"
cp "$PROJECT_DIR/run_app.py" "$STAGING_DIR/"
cp "$PROJECT_DIR/requirements-app.txt" "$STAGING_DIR/requirements.txt"

# Bundle upstream packages so the App can import them via PYTHONPATH
echo "  Bundling databricks-tools-core..."
mkdir -p "$STAGING_DIR/packages/databricks_tools_core"
cp -r "$REPO_ROOT/databricks-tools-core/databricks_tools_core/"* \
      "$STAGING_DIR/packages/databricks_tools_core/"

echo "  Bundling databricks-mcp-server..."
mkdir -p "$STAGING_DIR/packages/databricks_mcp_server"
cp -r "$REPO_ROOT/databricks-mcp-server/databricks_mcp_server/"* \
      "$STAGING_DIR/packages/databricks_mcp_server/"

# VERSION file — identity.py walks upward from packages/databricks_tools_core/ looking for it
cp "$REPO_ROOT/VERSION" "$STAGING_DIR/"

# Strip pyc files
find "$STAGING_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "$STAGING_DIR" -name "*.pyc" -delete 2>/dev/null || true

# Generate app.yaml
cat > "$STAGING_DIR/app.yaml" << APPYAML
command:
  - uvicorn
  - run_app:app
  - --host
  - "0.0.0.0"
  - --port
  - "\$DATABRICKS_APP_PORT"

env:
  - name: DATABRICKS_APPS_HOSTED
    value: "1"
  - name: DATABRICKS_HOST
    value: "${WORKSPACE_HOST}"
  - name: DATABRICKS_MCP_SQL_HOST
    value: "${WORKSPACE_HOST}"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "${WAREHOUSE_ID}"
  - name: PYTHONPATH
    value: "/app/python/source_code/packages"
APPYAML

echo -e "  ${GREEN}✓${NC} Staged to ${STAGING_DIR}"
echo ""

# ── Step 3: Create app ───────────────────────────────────────────────────────
echo -e "${YELLOW}[3/5] Ensuring app exists...${NC}"
if databricks apps get "$APP_NAME" $CLI_ARGS &>/dev/null; then
  echo -e "  ${GREEN}✓${NC} App '${APP_NAME}' already exists"
else
  echo "  Creating '${APP_NAME}'..."
  databricks apps create "$APP_NAME" $CLI_ARGS
  echo -e "  ${GREEN}✓${NC} Created"
fi
echo ""

# ── Step 4: Upload ───────────────────────────────────────────────────────────
echo -e "${YELLOW}[4/5] Uploading to workspace...${NC}"
databricks workspace import-dir "$STAGING_DIR" "$WORKSPACE_PATH" --overwrite $CLI_ARGS
echo -e "  ${GREEN}✓${NC} Uploaded"
echo ""

# ── Step 5: Deploy ───────────────────────────────────────────────────────────
echo -e "${YELLOW}[5/6] Deploying app...${NC}"
DEPLOY_OUT=$(databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH" $CLI_ARGS 2>&1)
echo "$DEPLOY_OUT"

APP_URL=$(databricks apps get "$APP_NAME" $CLI_ARGS --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))")

if echo "$DEPLOY_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status',{}).get('state')=='SUCCEEDED' else 1)" 2>/dev/null; then
  echo ""
  echo -e "${GREEN}╔══════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}║          Deployment successful!              ║${NC}"
  echo -e "${GREEN}╚══════════════════════════════════════════════╝${NC}"
  echo ""
  echo -e "  App URL:      ${GREEN}${APP_URL}${NC}"
  echo -e "  MCP endpoint: ${GREEN}${APP_URL}/mcp${NC}"
  echo ""
else
  echo ""
  echo -e "${RED}Deployment may have failed. Check logs:${NC}"
  echo -e "  databricks apps logs ${APP_NAME} ${CLI_ARGS}"
  exit 1
fi

# ── Step 6: Surface SP identity ──────────────────────────────────────────────
echo -e "${YELLOW}[6/6] Fetching app service principal...${NC}"
APP_SP_NAME=$(databricks apps get "$APP_NAME" $CLI_ARGS --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_name','unknown'))")
APP_SP=$(databricks apps get "$APP_NAME" $CLI_ARGS --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id','unknown'))")

echo ""
echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║  First-deploy checklist (one-time, persists across redeploys) ║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  App service principal: ${GREEN}${APP_SP_NAME}${NC}"
echo -e "  SP application ID:     ${GREEN}${APP_SP}${NC}  (use this for ACL grants)"
echo ""
echo -e "  Review the SP above, then complete these steps:"
echo ""
echo -e "  1. Grant READ on secret scope dbrix_mcp_secret:"
echo -e "     ${BLUE}databricks secrets put-acl dbrix_mcp_secret ${APP_SP} READ --profile ${PROFILE}${NC}"
echo ""
echo -e "  2. Grant CAN_USE on warehouse ${WAREHOUSE_ID} (Databricks UI):"
echo -e "     ${BLUE}${WORKSPACE_HOST}/sql/warehouses/${WAREHOUSE_ID}/permissions${NC}"
echo ""
echo -e "  3. Set authorization mode to 'On behalf of service principal' (Databricks UI):"
echo -e "     ${BLUE}${WORKSPACE_HOST}/apps/${APP_NAME}/authorization${NC}"
echo ""
