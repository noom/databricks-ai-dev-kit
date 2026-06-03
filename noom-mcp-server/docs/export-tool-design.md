# Design note: bulk query export (`export_query_to_file`)

## TL;DR
Returning large query results through the MCP blows up the model's context
window, and nothing in the existing toolset avoids it. This adds a new
customization-layer tool, `export_query_to_file`, which has the **server
process** atomically write results to local disk and return only a small
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
2. **Lazy, just-in-time streaming.** A generator walks the `next_chunk_index`
   chain, and each chunk's presigned link is downloaded *immediately* as it is
   produced (not collected up front). This keeps each URL freshly issued when
   used, so links don't age out mid-export, and it is memory-bounded (one chunk
   at a time) and never truncates. The CSV header arrives in the first chunk; an
   empty result yields a header-only file from the manifest schema.
3. **Atomic write.** Chunks stream to a temp file in the destination directory,
   which is moved into place only after the *last* chunk is written. If any
   download fails — or the wall-clock ceiling fires mid-stream — the temp file
   is discarded and the destination is left untouched. There is never a partial
   or empty file at the real path (a half-written CSV that looks complete is
   exactly the silent corruption this tool exists to prevent).
4. **Overwrite guard.** The early existence check is only a fast-fail; the
   authoritative guard is atomic at the move step. `overwrite=false` uses
   `os.link` (fails if the destination exists, even if it appeared *during* the
   query); `overwrite=true` uses `os.replace`. This closes the time-of-check /
   time-of-use gap across a long-running query.
5. **Timeout bounded by the tool ceiling.** The per-call `timeout` is clamped to
   the tool's 600s wall-clock ceiling, so the poll loop's own cancellation fires
   before FastMCP aborts the call — otherwise an over-large timeout would orphan
   the statement on the warehouse.
6. **What the model sees.** Only the manifest — not the rows.
7. **Governance preserved.** Reuses the existing chokepoints: Service-Principal
   client (creds from the `dbrix_mcp_secret` scope), forced warehouse override,
   and `mcp_user:<identity>` query tagging, plus a `mcp_tool:export_query_to_file`
   tag. Exports appear in `system.query.history` like any governed query.
8. **Path safety.** Writes are confined to `DATABRICKS_MCP_EXPORT_DIR` (default
   `~/databricks-mcp-exports`); `..` traversal and absolute escapes are rejected.
   Containment is also **re-checked immediately before writing**, so a symlink
   introduced under the base during the query can't redirect the write outside
   the sandbox.

### Correctness & safety properties (and where each is enforced)
| Property | Mechanism |
|---|---|
| No truncation of large results | `EXTERNAL_LINKS` + full `next_chunk_index` pagination (vs. upstream's first-chunk `INLINE`) |
| Memory-bounded | one chunk streamed at a time |
| No presigned-link expiry mid-export | lazy, just-in-time per-chunk download |
| No partial/empty file on failure | temp file + atomic move, discard-on-error |
| No silent clobber of an existing file | `os.link` no-clobber when `overwrite=false` |
| No orphaned warehouse statement | per-call timeout clamped to the 600s ceiling |
| No write outside the sandbox | up-front path validation + write-time containment re-check |
| No data through the model context | server writes the file; only the manifest is returned |

### Data lifecycle
There are two distinct "CSV" artifacts; only one is ours to manage.

| Artifact | Location | Lifecycle | Owner |
|---|---|---|---|
| Cloud-fetch staging | Databricks-managed cloud storage | Transient. Each presigned link carries an explicit `expiration` (the authoritative value; ~15 min observed in testing, **not** a documented constant). The staged result is held only briefly under Databricks' statement-result retention, then purged. | **Databricks** — we don't choose the path, create a table/volume, or extend it |
| Exported `.csv` | Engineer's local disk (`DATABRICKS_MCP_EXPORT_DIR`) | Deleted after `DATABRICKS_MCP_EXPORT_RETENTION_DAYS` (default 7; `0` disables); swept on startup and before each export. The sweep never follows symlinks or deletes outside the base. | **This tool** |

We persist nothing durable or self-owned in Databricks. The only artifact we
own is the local file, and because exports can contain prod PII it is **not**
retained indefinitely: `sweep_old_exports()` removes files older than the
retention window.

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
| `customization/export_query_patch.py` | New — tool, lazy chunk streaming, atomic write, overwrite guard, timeout clamp, path sandbox + retention sweep |
| `customization/tool_allowlist_patch.py` | Added `export_query_to_file` to `ALLOWED_TOOLS` |
| `run.py` | New Step 2b: registers the tool before the allowlist |
| `tests/test_export_query_patch.py` | New — unit tests (sandbox, header quoting, lazy chunk iteration, atomic write, overwrite guard, timeout clamp, retention, symlink hardening) |
| `.env.example` | Documented `DATABRICKS_MCP_EXPORT_DIR` and `DATABRICKS_MCP_EXPORT_RETENTION_DAYS` |
| `docs/export-tool-design.md` | This design note |

**Validation:** 65 unit tests pass, ruff check + format clean. Validated three
ways: the full `run.py` startup lifecycle (tool registered, survives the
allowlist, governed SP path), live warehouse exports (synthetic deterministic,
a real 122-column slice, empty → header-only), and a byte-for-byte identical
result from the local code path and the deployed MCP server on the real
experiment query.

## Follow-ups
- Optional Parquet output (`Format.ARROW_STREAM` + pyarrow) if a consumer
  prefers it over CSV.
- Engineers must restart their MCP client to pick up the tool after it ships.

---

### Changelog — what changed since the previous version of this note
The earlier draft described the v1 design. Since then, code review (Cursor
Bugbot) surfaced several correctness and safety issues that have been fixed; the
note now reflects the shipped behavior:

- **Atomic write (was: direct write).** The previous note said the server
  "writes the bytes straight to the local file." That left a partial/empty file
  at the destination if a download failed mid-stream. Now writes go to a temp
  file and are atomically moved into place only on full success.
- **Overwrite guard hardened.** The existence check ran once before the query; a
  file appearing during the query could still be clobbered. The move now uses
  `os.link` (no-clobber) when `overwrite=false`.
- **Lazy link fetching (was: eager collection).** Links are now fetched
  just-in-time per chunk so presigned URLs don't expire during long exports.
- **Timeout clamped to the 600s ceiling**, preventing orphaned warehouse
  statements when a caller passes an over-large timeout.
- **Write-time sandbox re-check** added, so a symlink introduced during the
  query can't redirect the write outside the export dir.
- **Retention / PII lifecycle added.** New `DATABRICKS_MCP_EXPORT_RETENTION_DAYS`
  (default 7); files are swept on startup and before each export. The sweep is
  symlink-hardened (never follows symlinks or deletes outside the base).
- **Lifecycle wording corrected.** The "~15 min" presigned-link figure is an
  *observation*, not a documented Databricks SLA — the per-response `expiration`
  field is authoritative.
- **Counts updated.** Six files (added `.env.example`, this doc); 65 unit tests
  (was "14 / 51").
