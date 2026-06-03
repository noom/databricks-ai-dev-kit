# Spec: Noom MCP Server — Databricks Apps Hosting

## Goal

Deploy the Noom-governed MCP server as a shared Databricks App so engineers connect to
a central hosted endpoint instead of running a local process. Governance controls
(SP-based SQL execution, per-user query tagging, tool allowlist) must remain identical
to the local mode.

## Context

### Current architecture (local mode)

Each engineer runs `uv run python run.py` as a subprocess on their laptop. The MCP
client (Cursor / Claude Desktop) connects via **stdio**. At startup:

1. `ensure_oauth_authenticated()` opens a browser OAuth flow to identify the user.
2. `patch_sql_executor()` monkey-patches `SQLExecutor` so all SQL runs as a governed
   Service Principal (SP), with `mcp_user:<email>` appended to `system.query.history`.
3. `apply_tool_allowlist()` removes unapproved tools from the FastMCP instance.
4. `mcp.run()` starts the stdio loop.

The user identity (`mcp_user:<email>`) is resolved once at startup from the local
OAuth cache and is effectively process-scoped (one process = one user).

### Why hosting changes things

| Concern | Local | Hosted |
|---|---|---|
| Transport | stdio | Streamable HTTP (`POST /mcp`) |
| User identity | Local OAuth cache, resolved at startup | HTTP header `X-Forwarded-User` injected per-request by Databricks Apps auth proxy |
| Auth gate | Browser OAuth flow | Databricks Apps handles OAuth — no browser needed |
| SP credential fetch | User's OAuth token fetches from secret scope | App's service principal fetches from secret scope |
| Concurrency | 1 user per process | Many users share one process → identity must be request-scoped |

### How the upstream builder app handles packaging

`databricks-builder-app/scripts/deploy.sh` copies `databricks_tools_core` and
`databricks_mcp_server` source into a staging `packages/` directory at deploy time,
then sets `PYTHONPATH=/app/python/source_code/packages` in `app.yaml`. The packages
are never checked into this fork. The same approach applies here — the deploy script
for `noom-mcp-server` will bundle the upstream packages at deploy time.

### Why not Databricks AI Gateway

AI Gateway is for LLM inference traffic (OpenAI-compatible format). It cannot proxy
MCP protocol (Streamable HTTP / SSE). Databricks Apps is the right product: it hosts
arbitrary Python web servers and provides a built-in OAuth proxy that injects the
authenticated user's identity as `X-Forwarded-User`.

---

## Design decisions

**1. Separate `hosting/` from `customization/`**

`customization/` = monkey-patches applied to the upstream server at import time.
`hosting/` = HTTP serving infrastructure for the Databricks Apps deployment.

These are different concerns. `customization/` is unchanged in meaning; `hosting/`
is new and scoped only to the hosted entrypoint.

**2. Per-request identity via `ContextVar`**

In local mode, identity is process-scoped (one user, resolved at startup). In hosted
mode, many users share one process. A `contextvars.ContextVar` is set per-request
from `X-Forwarded-User` before any tool logic runs. `get_mcp_user_identity()` in
`sql_executor_patch.py` reads from this ContextVar when hosted.

**3. Minimal diff to `customization/`**

Only two files in `customization/` change:
- `auth_guard_patch.py`: skip browser OAuth when `DATABRICKS_APPS_HOSTED=1`.
- `sql_executor_patch.py`: `get_mcp_user_identity()` reads from ContextVar (falls
  back to existing logic for local mode).

All other patches (`version_check`, `tool_allowlist`, `sql_timeout`, SP client
override, warehouse pin) are unchanged.

**4. `run.py` is untouched**

Local dev continues to work exactly as today. The hosted entrypoint is a new file
(`run_app.py`) that uses the `hosting/` layer.

---

## Target file structure

```
noom-mcp-server/
  customization/                  # unchanged purpose — patches to upstream
    patches.py
    auth_guard_patch.py           # +2 lines: bypass browser flow when DATABRICKS_APPS_HOSTED=1
    sql_executor_patch.py         # get_mcp_user_identity() reads ContextVar when hosted
    sql_timeout_patch.py
    tool_allowlist_patch.py
    version_check.py

  hosting/                        # new — Databricks Apps serving layer
    __init__.py
    request_identity.py           # ContextVar[str] + set_user_from_request(headers)
    middleware.py                 # ASGI middleware: extracts X-Forwarded-User, sets ContextVar

  run.py                          # local dev entrypoint — unchanged
  run_app.py                      # hosted entrypoint: apply_all_patches → allowlist →
                                  #   timeout patch → mount hosting/ middleware → mcp.run(transport="streamable-http")
  app.yaml                        # Databricks Apps manifest
  scripts/
    deploy.sh                     # stage packages/ → workspace import → apps deploy
```

---

## Changes in detail

### `hosting/request_identity.py`

```python
import contextvars

_current_user: contextvars.ContextVar[str] = contextvars.ContextVar("mcp_user", default="unknown")

def set_user_from_request(headers: dict) -> None:
    email = headers.get("x-forwarded-user", "unknown")
    _current_user.set(email)

def get_current_mcp_user() -> str:
    return _current_user.get()
```

### `hosting/middleware.py`

ASGI middleware that runs before every MCP request. Extracts `X-Forwarded-User` from
the request headers and calls `set_user_from_request()` to populate the ContextVar.
Non-HTTP scopes (lifespan, websocket) pass through unchanged.

### `customization/auth_guard_patch.py`

```python
def ensure_oauth_authenticated() -> None:
    if os.environ.get("DATABRICKS_APPS_HOSTED"):
        logger.info("Running in Databricks Apps — skipping browser OAuth (identity from request headers)")
        return
    # existing browser flow unchanged
    ...
```

### `customization/sql_executor_patch.py` — `get_mcp_user_identity()`

```python
def get_mcp_user_identity() -> str:
    # Hosted mode: identity arrives per-request via X-Forwarded-User
    if os.environ.get("DATABRICKS_APPS_HOSTED"):
        from hosting.request_identity import get_current_mcp_user
        return get_current_mcp_user()
    # Local mode: unchanged
    from databricks_tools_core.auth import get_current_username
    username = get_current_username()
    if username:
        return username
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    return f"sp:{client_id}" if client_id else "unknown"
```

### `run_app.py`

Same patch sequence as `run.py` (version check → patch SQL executor → OAuth guard →
allowlist → timeout ceiling), then mounts the identity middleware and runs with
Streamable HTTP transport:

```python
os.environ["DATABRICKS_APPS_HOSTED"] = "1"
# ... apply_all_patches(), allowlist, timeout ...
from hosting.middleware import IdentityMiddleware
mcp_asgi = mcp.http_app(path="/mcp", stateless_http=True)
app = IdentityMiddleware(mcp_asgi)
# run via uvicorn externally (app.yaml command)
```

### `app.yaml`

```yaml
command:
  - uvicorn
  - run_app:app
  - --host
  - "0.0.0.0"
  - --port
  - "$DATABRICKS_APP_PORT"

env:
  - name: DATABRICKS_APPS_HOSTED
    value: "true"
  - name: DATABRICKS_HOST
    value: "https://noom-prod.cloud.databricks.com"
  - name: DATABRICKS_MCP_SQL_HOST
    value: "https://noom-prod.cloud.databricks.com"
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom:
      secretScope: dbrix_mcp_secret
      secretKey: warehouse-id
  - name: PYTHONPATH
    value: "/app/python/source_code/packages"
```

### `scripts/deploy.sh`

Mirrors `databricks-builder-app/scripts/deploy.sh` pattern:

1. Stage `customization/`, `hosting/`, `run_app.py`, `app.yaml` into a temp dir.
2. Copy upstream packages:
   ```bash
   cp -r ../databricks-tools-core/databricks_tools_core/ staging/packages/databricks_tools_core/
   cp -r ../databricks-mcp-server/databricks_mcp_server/  staging/packages/databricks_mcp_server/
   ```
3. `databricks workspace import-dir staging/ $WORKSPACE_PATH --overwrite`
4. `databricks apps deploy $APP_NAME --source-code-path $WORKSPACE_PATH`

---

## Permissions required (admin setup)

The Databricks App's service principal (auto-created by `databricks apps create`) needs:

- **READ** on secret scope `dbrix_mcp_secret` — to fetch SQL SP credentials and
  warehouse ID.
- **Can use** on the pinned SQL warehouse.

No change to the SQL SP itself; it already has the permissions needed for query
execution.

---

## Out of scope

- MCP client configuration (Cursor / Claude Desktop remote MCP setup) — separate doc.
- Multi-workspace routing — hosted app targets prod only, same as local mode.
- The upstream `databricks-builder-app` MCP gateway — that is a different product with
  a UI; this spec is for a standalone MCP-only App.
