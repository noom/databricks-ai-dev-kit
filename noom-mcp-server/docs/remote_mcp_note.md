# Databricks Apps — Remote MCP Notes

## Status (as of 2026-06-04)

| Item | Status |
|---|---|
| Hosted entrypoint (`run_app.py`) | Done |
| Deploy script (`scripts/deploy.sh`) | Done — supports `--env dev\|prod` |
| App deployed to dev | Done — `mcp-noom-dev` on `noom-dev.cloud.databricks.com` |
| App SP excluded from governance cleanup | **Pending** — must be added to ignore list |
| App SP granted READ on `dbrix_mcp_secret` | Done (dev: `fe62b38f-c398-43bc-a8dc-191e0974a2e2`) |
| Warehouse CAN_USE grant for app SP | **Pending** |
| Authorization mode set to "on behalf of SP" | **Pending** |
| MCP client config docs | Deferred — experimentation still in progress |

---

## Platform facts

| Fact | Impact |
|---|---|
| OAuth proxy is automatic | All requests are authenticated via Databricks SSO before reaching the app. `X-Forwarded-User: <email>` is injected per-request — trusted, not user-supplied. |
| Each app gets an auto-created SP | SP is provisioned on `apps create`. Cannot be replaced with an existing SP. |
| SP is tied to the app, not the deployment | `apps deploy` reuses the same SP. Only `apps delete` + `apps create` produces a new SP. |
| `app.yaml` supports only `command` and `env` | No `resources` section in standalone mode — permission grants are always manual. |
| Authorization mode is UI-only | "On behalf of service principal" vs "on behalf of user" cannot be set via CLI or `app.yaml`. Must be configured in the Databricks UI once after first deploy. |

## What we need to do

### 1. Exclude the app SP from the governance cleanup script

Noom's SP governance framework will delete the app's SP if it isn't excluded, breaking the app. After every `apps create`, add the SP's application ID (UUID printed by `deploy.sh` Step 6) to the ignore list.

### 2. Grant the app SP READ on `dbrix_mcp_secret`

The app SP needs this to fetch the SQL SP credentials at startup:

```bash
databricks secrets put-acl dbrix_mcp_secret <sp-application-id> READ --profile <profile>
```

The SP application ID is the UUID shown in `deploy.sh` Step 6 output.

### 3. Set authorization mode in the UI (first deploy only)

Go to: `https://<workspace>/apps/<app-name>/authorization`  
Select: **On behalf of service principal**

Persists across redeployments unless the app is deleted.

## If the SP gets deleted

The app is unrecoverable. Steps:

```bash
databricks apps delete <app-name> --profile <profile>
bash scripts/deploy.sh <app-name> --env <env>
```

Then redo steps 1–3 above with the new SP's application ID.

## Test script

`test-mcp-databricks-dev.sh` (repo root) sends raw MCP requests to the hosted app using your local Databricks OAuth token.

```bash
# List tools
bash test-mcp-databricks-dev.sh

# Run a SQL query
bash test-mcp-databricks-dev.sh noom-databricks-dev execute_sql "SELECT 1 AS test"

# List warehouses
bash test-mcp-databricks-dev.sh noom-databricks-dev list_warehouses
```

The script uses `databricks auth token -p <profile>` and passes it as `Authorization: Bearer <token>` to the MCP endpoint.

---

## Environments

| Env | Profile | Host | Warehouse |
|---|---|---|---|
| dev | `dev` | `noom-dev.cloud.databricks.com` | `12ce469e5394ac8b` |
| prod | `prod` | `noom-prod.cloud.databricks.com` | `575c0a43969584a4` |

```bash
bash scripts/deploy.sh mcp-noom-dev --env dev
bash scripts/deploy.sh mcp-noom-prod --env prod
```
