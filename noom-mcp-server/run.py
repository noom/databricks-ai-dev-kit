"""Entry point for the Noom-governed Databricks MCP server.

Prerequisites:
    databricks auth login --host https://<your-workspace>.cloud.databricks.com
    cp .env.example .env  # fill in DATABRICKS_HOST, DATABRICKS_MCP_SQL_HOST,
                          # and DATABRICKS_WAREHOUSE_ID

Start with:
    uv run --env-file .env python run.py

Or via MCP client config (Claude Desktop / Cursor):
    {
        "mcpServers": {
            "noom-databricks": {
                "command": "uv",
                "args": [
                    "run",
                    "--directory", "/path/to/databricks-ai-dev-kit/noom-mcp-server",
                    "--env-file", "/path/to/databricks-ai-dev-kit/noom-mcp-server/.env",
                    "python", "run.py"
                ]
            }
        }
    }

Required env vars (see .env.example):
    DATABRICKS_HOST              — Workspace URL for OAuth identity resolution
    DATABRICKS_MCP_SQL_HOST      — Workspace URL for SQL execution (usually same as above)
    DATABRICKS_WAREHOUSE_ID      — SQL warehouse ID; all queries are forced to this warehouse

SP credentials are fetched automatically from the dbrix_mcp_secret Databricks secret scope.
See DEVELOPMENT.md for admin setup and dev/CI credential overrides.
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("customization")

# ---------------------------------------------------------------------------
# Step 1: Apply governance patches BEFORE the upstream server imports tools.
#
# patch_sql_executor() patches SQLExecutor at the class level — it only needs
# the databricks-tools-core package to be importable, not the MCP server.
#
# check_pat_rejected() makes a live Databricks API call, so it requires valid
# workspace credentials to be configured in the environment.
# ---------------------------------------------------------------------------

from customization.patches import apply_all_patches, UpstreamChangedError  # noqa: E402

try:
    apply_all_patches()
except UpstreamChangedError as exc:
    logger.error(
        "UPSTREAM VERSION CHANGED — server will not start.\n%s",
        exc,
    )
    sys.exit(2)
except RuntimeError as exc:
    logger.error("Governance check failed — server will not start: %s", exc)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 2: Import the upstream MCP server.
#
# Importing this module creates the FastMCP instance and registers all tools
# (including execute_sql / execute_sql_multi) via @mcp.tool decorators.
# The SQLExecutor patches are already in place at this point.
# ---------------------------------------------------------------------------

from databricks_mcp_server.server import mcp  # noqa: E402

# ---------------------------------------------------------------------------
# Step 3: Run.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting Noom MCP server (governed Databricks MCP)")
    mcp.run()
