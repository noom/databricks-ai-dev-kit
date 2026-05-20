# Noom — Databricks AI Dev Kit

This is Noom's **staging fork** of [databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit).
Its purpose is to extend the upstream Databricks MCP server with Noom-specific SQL governance
controls before distributing it to engineers.

## What Noom maintains here

- **`noom-mcp-server/`** — The only directory Noom actively changes.
  Contains a monkey-patch extension layer that wraps the upstream MCP server with:
  - PAT authentication rejection (OAuth only)
  - Service Principal SQL execution (all SQL runs as the governed SP, not the user)
  - Per-user `mcp_user:<email>` query tagging in `system.query.history`

Everything else (`databricks-mcp-server/`, `databricks-tools-core/`, `databricks-skills/`, etc.)
is upstream code and should not be modified here.

## How to set up the MCP server (for engineers)

```bash
# 1. Clone this repo
git clone https://github.com/noom/databricks-ai-dev-kit.git
cd databricks-ai-dev-kit/noom-mcp-server

# 2. Authenticate to Databricks (OAuth — PAT tokens are rejected)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 3. Create the env file
cp .env.example .env
# Edit .env: set DATABRICKS_HOST, DATABRICKS_MCP_SQL_HOST, DATABRICKS_WAREHOUSE_ID

# 4. Install dependencies
uv sync

# 5. Run the server
uv run --env-file .env python run.py
```

See [`noom-mcp-server/README.md`](noom-mcp-server/README.md) for full setup instructions
and [`noom-mcp-server/DEVELOPMENT.md`](noom-mcp-server/DEVELOPMENT.md) for contributing.

## Do NOT use the upstream installers

`install.sh`, `install.ps1`, and `databricks-mcp-server/setup.sh` are **blocked** in this
fork. They install the unpatched upstream server without Noom's governance controls.

Use `noom-mcp-server/` instead.

## Configuring your MCP client

Add to your Cursor / Claude Desktop `mcp.json`:

```json
{
  "mcpServers": {
    "noom-databricks": {
      "command": "uv",
      "args": [
        "run",
        "--directory", "/absolute/path/to/databricks-ai-dev-kit/noom-mcp-server",
        "--env-file", "/absolute/path/to/databricks-ai-dev-kit/noom-mcp-server/.env",
        "python", "run.py"
      ]
    }
  }
}
```

## How to contribute

Work only inside `noom-mcp-server/`. Open PRs against the `main` branch of
`noom/databricks-ai-dev-kit` — **not** against the upstream
`databricks-solutions/ai-dev-kit`. See [`noom-mcp-server/DEVELOPMENT.md`](noom-mcp-server/DEVELOPMENT.md)
for the `gh pr create` command that targets the right repo.

## Upgrading the upstream

See the "Upgrading the upstream" section in [`noom-mcp-server/README.md`](noom-mcp-server/README.md).
