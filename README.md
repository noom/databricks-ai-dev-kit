# Noom — Databricks AI Dev Kit

This is Noom's **governed fork** of [databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit).

## Getting started

**→ Follow [`noom-mcp-server/README.md`](noom-mcp-server/README.md)**

That's the only component Noom engineers need. It installs MCP client for Claude Desktop, Cursor, and Claude Code.

## Why a fork?

Noom wraps the upstream MCP server with three SQL governance controls:

- **PAT rejection** — Enforce OAuth; personal access tokens are blocked at startup
- **Service Principal execution** — all SQL runs as a governed SP, not your personal credentials
- **Per-user query tagging** — every query is tagged `mcp_user:<email>` in `system.query.history` for audit and cost attribution

## PII safety

The MCP does not mask or redact PII itself. PII protection is enforced at the storage layer by Unity Catalog's ABAC (Attribute-Based Access Control) column-level security policies. The governed Service Principal is granted access only to non-PII columns, so those policies ensure SQL results are PII-free by construction — the MCP never sees the data in the first place.

If your work genuinely requires querying PII data, reach out to the data platform team to discuss access options outside this MCP.

## What NOT to use

The scripts below install the **unpatched upstream server** without Noom's governance controls. Do not use them:

- `install.sh`
- `install.ps1`
- `databricks-mcp-server/setup.sh`

## How the extension works

Noom's code lives entirely in `noom-mcp-server/`. **No upstream file is modified.**

Instead, `run.py` applies a sequence of monkey-patches to the upstream Python objects before the MCP server starts. The upstream server is then imported and run unchanged.

```
run.py
  ├── apply_all_patches()          ← all Noom changes, runs first
  │     ├── check_upstream_version   abort if upstream changed since last validation
  │     ├── patch_sql_executor       swap SQLExecutor to use the SP client; tag queries
  │     ├── patch_get_best_warehouse lock all queries to the designated warehouse
  │     └── ensure_oauth_authenticated  reject PATs; open browser OAuth flow if needed
  ├── register_export_query_tool   add bulk-export tool that keeps data out of model context
  ├── apply_tool_allowlist         remove upstream tools not on Noom's approved list
  ├── apply_sql_timeout_ceiling    raise the 60 s per-call ceiling for analytical queries
  └── mcp.run()                    ← upstream FastMCP server, unmodified
```

**Why monkey-patching instead of forking the code?**
Patching at the object level means zero diff against the upstream files. When upstream ships a bug fix or new feature, `git merge upstream/main` applies cleanly — Noom's changes don't conflict because they touch no upstream line. The version pin (`PATCHED_UPSTREAM_VERSION` in `version_check.py`) forces an explicit re-validation on every upstream bump; the server refuses to start if the pin is stale.

See [`noom-mcp-server/DEVELOPMENT.md`](noom-mcp-server/DEVELOPMENT.md) for full architecture and design rationale.

## Upstream components (read-only)

Everything outside `noom-mcp-server/` is upstream code. Do not modify it in this fork — changes belong in the [upstream repo](https://github.com/databricks-solutions/ai-dev-kit).

| Directory | Description |
|---|---|
| `databricks-mcp-server/` | Upstream MCP server (used as a dependency) |
| `databricks-tools-core/` | Upstream Python library |
| `databricks-skills/` | Upstream skill files |
| `databricks-builder-app/` | Upstream web app |

To pull in upstream bug fixes or new features, see the "Sync the upstream" section in [`noom-mcp-server/README.md`](noom-mcp-server/README.md).
