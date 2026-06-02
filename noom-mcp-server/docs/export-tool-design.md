# Design note: bulk query export (`export_query_to_file`)

## TL;DR
Returning large query results through the MCP blows up the model's context
window, and nothing in the existing toolset avoids it. This adds a new
customization-layer tool, `export_query_to_file`, which has the **server
process** write results straight to local disk and return only a small
manifest — so the row data never passes through the model's context at all.

## The core problem: how MCP tool results reach the model
A tool call's return value becomes part of the conversation — those bytes are
tokenized into the model's context window. That's fine when the model is the
*consumer* of the data (reading/reasoning over a small result). It breaks when
the model is just a *courier* moving bulk data somewhere else.

Concretely: a real analytical query (the `Meristem ups_ex360_med` experiment
grid) came back as **198 rows × 35 columns ≈ 68 KB ≈ 37,000 tokens**. That
single result exceeded read limits and had to spill to a temp file. Every query
like it does the same. This is not a bug — it's the inherent shape of returning
data through a tool result.

## Why this keeps happening (it's structural, not a one-off)
- **A tool result is tokenized into context — by design.** There is no "stream"
  or "view" mode; whatever a tool returns becomes conversation tokens.
- **Context is a fixed, shared budget.** Result rows compete with instructions,
  history, and every other tool call for the same space.
- **It scales the wrong way.** More rows = more tokens = more cost/latency,
  until it overflows. Analytical/EDA work trends toward *larger* results.
- **The model is a lossy, expensive conduit.** Downstream tools need the exact
  bytes; a model can round, drop, or summarize. Even when a result "fits,"
  fidelity isn't guaranteed.

So any non-trivial extract, by anyone, hits the same wall. The durable fix is to
stop routing data through the model at all.

## Approaches considered (and why rejected)
1. **Work around it with current tools** — No. Every result-returning tool
   serializes into context by design; `output_format` only changes formatting,
   not destination. The harness overflow temp-file is a side-effect, not an
   export; reading it back re-bloats context.
2. **Keep data in Databricks (CTAS + consumer reads the table)** — Good for the
   in-Databricks model app, but local tooling (viz, notebooks) needs local
   files; can't funnel everything through the model app.
3. **Subagent fetches the data** — Hides the bloat in a child context but
   doesn't eliminate it, and an LLM can't losslessly transcribe a large result
   (silent corruption). The only correct version runs *code* to move bytes — at
   which point the subagent adds nothing.
4. **Bundle a separate connector script with the plugin** — Works, but every
   user must install + auth a separate client. The MCP server already holds a
   governed connection.

## The chosen fix: a write-to-disk MCP tool
An MCP server is a local process with filesystem access. A tool may have a side
effect (write a file) and return only a small confirmation. So data flows
**warehouse → server process → local disk**, with only a manifest
(`{path, row_count, bytes, columns, format}`) returning through the protocol
into context.

### How it works
1. **Execution disposition.** Does *not* reuse `execute_sql` (which uses the
   `INLINE` disposition and reads only the first chunk — it truncates large
   results). Instead it issues the statement with `disposition=EXTERNAL_LINKS`
   and `format=CSV`. Databricks writes the result to cloud storage as CSV chunk
   files and returns presigned download URLs.
2. **Server-side streaming.** The server downloads each presigned chunk and
   writes the bytes to the local file, following the `next_chunk_index` chain.
   Memory-bounded (one chunk at a time), never truncates. The CSV header arrives
   in the first chunk; an empty result yields a header-only file from the
   manifest schema.
3. **What the model sees.** Only the manifest — not the rows.
4. **Governance preserved.** Reuses the existing chokepoints: Service-Principal
   client, forced warehouse override, and `mcp_user:<identity>` query tagging,
   plus a `mcp_tool:export_query_to_file` tag.
5. **Path safety.** Writes are confined to `DATABRICKS_MCP_EXPORT_DIR`
   (default `~/databricks-mcp-exports`); `..` traversal and absolute paths that
   escape the base are rejected.

### Data lifecycle
There are two distinct "CSV" artifacts; only one is ours to manage.

| Artifact | Location | Lifecycle | Owner |
|---|---|---|---|
| Cloud-fetch staging | Databricks-managed cloud storage | Transient — presigned links expire in ~15 min (measured); the staged result is held only briefly under Databricks' statement-result retention, then purged | **Databricks** — we don't choose the path, create a table/volume, or extend it |
| Exported `.csv` | Engineer's local disk (`DATABRICKS_MCP_EXPORT_DIR`) | Deleted after `DATABRICKS_MCP_EXPORT_RETENTION_DAYS` (default 7); swept on startup and before each export | **This tool** |

We persist nothing durable or self-owned in Databricks. The only artifact we
own is the local file, and because exports can contain prod PII it is **not**
retained indefinitely: `sweep_old_exports()` removes files older than the
retention window (set the env var to `0` to disable).

### Governing principle
The MCP layer is a **control plane, not a data plane.** It orchestrates and
passes *handles* (a table name, or a file path); bulk bytes move
warehouse → storage → disk underneath, never through the model.
**Pass the path, not the payload.**

## What changed
All inside `noom-mcp-server/` (upstream untouched), same customization-layer
pattern as the SQL-timeout PR (#18):

| File | Change |
|---|---|
| `customization/export_query_patch.py` | New — tool, chunk streaming, path sandbox |
| `customization/tool_allowlist_patch.py` | Added `export_query_to_file` to `ALLOWED_TOOLS` |
| `run.py` | New Step 2b: registers the tool before the allowlist |
| `tests/test_export_query_patch.py` | New — 14 unit tests |
| `.env.example` | Documented `DATABRICKS_MCP_EXPORT_DIR` |

**Validation:** full unit suite passes, lint clean, and the path is
live-validated against the warehouse (synthetic deterministic result, a real
122-column table slice, and an empty result → header-only file).

## Follow-ups
- Optional Parquet output (`Format.ARROW_STREAM` + pyarrow) if a consumer
  prefers it over CSV.
