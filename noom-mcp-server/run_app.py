"""Hosted entrypoint for Databricks Apps deployment.

Exposes an ASGI 'app' object that uvicorn serves. Transport: Streamable HTTP
at POST /mcp. User identity arrives per-request via X-Forwarded-User injected
by the Databricks Apps OAuth proxy — no browser flow required.

Local development uses run.py (stdio transport). This file is for the hosted
deployment only.

Start (via app.yaml):
    uvicorn run_app:app --host 0.0.0.0 --port $DATABRICKS_APP_PORT

Required env vars (set in app.yaml):
    DATABRICKS_APPS_HOSTED       Set to "1" — signals hosted mode to patches.
    DATABRICKS_HOST              Workspace URL (used by SDK for identity/secrets).
    DATABRICKS_MCP_SQL_HOST      Workspace URL for SQL execution.
    DATABRICKS_WAREHOUSE_ID      Warehouse all queries are forced to run on.
    PYTHONPATH                   Must include the packages/ directory.
"""

import logging
import os
import sys

# Signal hosted mode before any patch code loads.
os.environ["DATABRICKS_APPS_HOSTED"] = "1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("customization")

# ---------------------------------------------------------------------------
# Step 1: Apply governance patches.
# ---------------------------------------------------------------------------

from customization.patches import apply_all_patches, UpstreamChangedError  # noqa: E402

try:
    apply_all_patches()
except UpstreamChangedError as exc:
    logger.error("UPSTREAM VERSION CHANGED — server will not start.\n%s", exc)
    sys.exit(2)
except RuntimeError as exc:
    logger.error("Governance check failed — server will not start: %s", exc)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 2: Import upstream server (registers all tools).
# ---------------------------------------------------------------------------

from databricks_mcp_server.server import mcp  # noqa: E402

# ---------------------------------------------------------------------------
# Step 3: Apply tool allowlist and timeout ceiling.
# ---------------------------------------------------------------------------

from customization.tool_allowlist_patch import apply_tool_allowlist  # noqa: E402
from customization.sql_timeout_patch import apply_sql_timeout_ceiling  # noqa: E402

apply_tool_allowlist(mcp)
apply_sql_timeout_ceiling(mcp)

# ---------------------------------------------------------------------------
# Step 4: Build the ASGI app.
#
# mcp.http_app() returns a Streamable HTTP ASGI app. IdentityMiddleware wraps
# it to extract X-Forwarded-User per request before any tool logic runs.
# Non-HTTP scopes (lifespan) pass through so FastMCP starts cleanly.
# ---------------------------------------------------------------------------

from hosting.middleware import IdentityMiddleware  # noqa: E402

mcp_asgi = mcp.http_app(path="/mcp", stateless_http=True)
app = IdentityMiddleware(mcp_asgi)

logger.info("Noom MCP server (hosted) ready — POST /mcp")
