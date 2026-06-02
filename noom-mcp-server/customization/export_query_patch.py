"""Export-query-to-file tool.

Adds a customization-layer MCP tool, ``export_query_to_file``, that runs a SQL
query and writes the **full** result set to a local file (CSV today), returning
only a small manifest (``{path, row_count, bytes, columns, format}``) to the
caller.

Why this exists
---------------
The upstream ``execute_sql`` tool serializes results back *through the MCP
protocol* into the model's context.  That is correct for interactive reads, but
it is the wrong shape for bulk extraction feeding a downstream model or local
tooling (data viz, notebooks): even a modest result set blows up the context
window, and large results trip the protocol's size limit.

This tool keeps the bytes out of the model entirely.  The **server process**
(which has local filesystem access) downloads the result and writes the file;
the model only ever receives a manifest.  Local tools then read the file off
disk.  Pass the path, not the payload.

Why not wrap SQLExecutor.execute()
----------------------------------
``SQLExecutor.execute()`` uses the Statement Execution API with the **INLINE**
disposition and ``_extract_results`` reads only ``result.data_array`` — the
*first chunk*.  It never paginates, so it (a) materializes rows as a list of
dicts in server RAM and (b) silently truncates large results to the first
chunk.  Both are unacceptable for a model-feeding extraction path.

Instead this tool issues the statement through the same governed SP client but
with the **EXTERNAL_LINKS** disposition and ``format=CSV``.  Databricks writes
the result to cloud storage as CSV chunks and returns presigned URLs; the
server streams those chunks straight to the local file.  This is memory-bounded
(one chunk at a time), never truncates, and avoids any lossy reformatting since
Databricks emits the CSV itself.

Governance
----------
Execution reuses the existing governance chokepoints from
``sql_executor_patch``:
  - ``get_sql_sp_client()``      → Service Principal credentials
  - ``get_sql_warehouse_id()``   → forced warehouse override
  - ``get_mcp_user_identity()``  → ``mcp_user:<identity>`` query tag

so exports show up in ``system.query.history`` exactly like ``execute_sql``
calls, plus a ``mcp_tool:export_query_to_file`` tag for filtering.

Path safety
-----------
For broad distribution the tool MUST NOT write to arbitrary local paths.  All
writes are confined to a single base directory:
  - ``DATABRICKS_MCP_EXPORT_DIR`` if set, else
  - ``~/databricks-mcp-exports`` (created on demand).
``output_path`` is interpreted relative to that base; absolute paths and ``..``
traversal that escape the base are rejected.
"""

import csv
import logging
import os
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Wall-clock ceiling (seconds) for the export tool call.  Bulk exports of large
# result sets can legitimately run longer than an interactive query, so this
# matches the raised SQL ceiling rather than the upstream 60s default.
EXPORT_TOOL_TIMEOUT_CEILING = 600

# Default per-call statement timeout (seconds) — overridable via the tool's
# ``timeout`` argument, bounded by the ceiling above.
_DEFAULT_STATEMENT_TIMEOUT = 300

# Default retention window (days) for files in the export directory. Exported
# files can contain prod PII, so they are not kept indefinitely: a sweep removes
# files older than this on startup and before each export. Override with
# DATABRICKS_MCP_EXPORT_RETENTION_DAYS; set it to 0 (or negative) to disable.
_DEFAULT_RETENTION_DAYS = 7

_SUPPORTED_FORMATS = ("csv",)


# ---------------------------------------------------------------------------
# Path sandboxing
# ---------------------------------------------------------------------------


def get_export_base_dir() -> Path:
    """Return the (created) base directory all exports are confined to.

    ``DATABRICKS_MCP_EXPORT_DIR`` if set, else ``~/databricks-mcp-exports``.
    """
    configured = os.environ.get("DATABRICKS_MCP_EXPORT_DIR")
    base = Path(configured).expanduser() if configured else Path.home() / "databricks-mcp-exports"
    base = base.resolve()
    base.mkdir(parents=True, exist_ok=True)
    return base


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


def get_export_retention_days() -> int:
    """Return the export-file retention window in days (default 7; 0 disables).

    Read from ``DATABRICKS_MCP_EXPORT_RETENTION_DAYS``. Invalid values fall back
    to the default with a warning.
    """
    raw = os.environ.get("DATABRICKS_MCP_EXPORT_RETENTION_DAYS")
    if raw is None or not raw.strip():
        return _DEFAULT_RETENTION_DAYS
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid DATABRICKS_MCP_EXPORT_RETENTION_DAYS=%r; using default %d.",
            raw,
            _DEFAULT_RETENTION_DAYS,
        )
        return _DEFAULT_RETENTION_DAYS


def sweep_old_exports(base_dir: Optional[Path] = None, retention_days: Optional[int] = None) -> int:
    """Delete files in the export dir older than the retention window.

    Exported CSVs can contain prod PII, so they are not retained indefinitely.
    A retention of <= 0 disables the sweep. Empty subdirectories left behind are
    pruned. Errors removing an individual path are logged, not raised — a sweep
    failure must never block an export.

    Args:
        base_dir: Override for the export base dir (tests).
        retention_days: Override for the retention window (tests).

    Returns:
        The number of files removed.
    """
    days = get_export_retention_days() if retention_days is None else retention_days
    if days <= 0:
        return 0

    base = (base_dir or get_export_base_dir()).resolve()
    cutoff = time.time() - days * 86400
    removed = 0

    for path in base.rglob("*"):
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError as exc:
            logger.warning("export retention sweep: could not remove %s: %s", path, exc)

    # Prune now-empty subdirectories, deepest first (never the base itself).
    for path in sorted(
        (p for p in base.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        try:
            if not any(path.iterdir()):
                path.rmdir()
        except OSError:
            pass

    if removed:
        logger.info(
            "export retention sweep: removed %d file(s) older than %d day(s) from %s",
            removed,
            days,
            base,
        )
    return removed


def resolve_export_path(output_path: str, base_dir: Optional[Path] = None) -> Path:
    """Resolve ``output_path`` against the export base dir, rejecting escapes.

    Args:
        output_path: Caller-supplied path, interpreted relative to the base dir.
            Absolute paths are allowed only if they fall inside the base dir.
        base_dir: Override for the base dir (tests).  Defaults to
            ``get_export_base_dir()``.

    Returns:
        The absolute, sandbox-checked destination path.

    Raises:
        ValueError: If the resolved path escapes the base dir, or the filename
            is empty / not a recognised extension.
    """
    if not output_path or not output_path.strip():
        raise ValueError("output_path must be a non-empty filename.")

    base = (base_dir or get_export_base_dir()).resolve()

    candidate = Path(output_path)
    combined = candidate if candidate.is_absolute() else base / candidate
    resolved = combined.resolve()

    # Containment check: resolved must be strictly under the base dir.
    if base not in resolved.parents:
        # Either it escapes the base dir, or it *is* the base dir (no filename).
        if resolved == base:
            raise ValueError("output_path must include a filename, not just the export directory.")
        raise ValueError(
            f"output_path {output_path!r} resolves outside the export directory "
            f"({base}). Writes are confined to that directory; use a relative path "
            f"or set DATABRICKS_MCP_EXPORT_DIR."
        )
    return resolved


# ---------------------------------------------------------------------------
# Result streaming
# ---------------------------------------------------------------------------


def _collect_external_links(client, statement_id: str, first_result) -> List[Any]:
    """Walk the chunk chain, returning all external links in row order."""
    links: List[Any] = []
    result = first_result
    while result is not None and result.external_links:
        links.extend(result.external_links)
        nxt = result.next_chunk_index
        if nxt is None:
            break
        result = client.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id, chunk_index=nxt
        )
    # Order defensively by chunk_index (fallback row_offset) so rows are in order.
    links.sort(key=lambda link: (link.chunk_index or 0, link.row_offset or 0))
    return links


def _download_link(link) -> bytes:
    """Download one presigned external link, honouring its http_headers.

    The link is a presigned cloud-storage URL — Databricks workspace auth must
    NOT be attached; only the headers the link itself specifies are sent.
    """
    req = urllib.request.Request(link.external_link)
    for key, value in (link.http_headers or {}).items():
        req.add_header(key, value)
    with urllib.request.urlopen(req) as resp:  # noqa: S310 - presigned Databricks URL
        return resp.read()


def _write_csv(dest: Path, columns: List[str], links: List[Any]) -> None:
    """Stream each CSV chunk's bytes to ``dest`` in row order.

    Databricks EXTERNAL_LINKS + CSV chunks already include the column header
    (in the first chunk), so the chunk bytes are written as-is — synthesizing a
    header would duplicate it.

    The one exception is an empty result: Databricks returns no external links,
    so there are no bytes to stream. In that case a header-only CSV is written
    (derived from the manifest schema) so the file is still valid and carries
    the column names for downstream tools.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as fh:
        if not links:
            fh.write(_csv_header_line(columns).encode("utf-8"))
            return
        for link in links:
            fh.write(_download_link(link))


def _csv_header_line(columns: List[str]) -> str:
    import io

    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(columns)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------


def _run_export(
    sql_query: str,
    output_path: str,
    fmt: str,
    catalog: Optional[str],
    schema: Optional[str],
    overwrite: bool,
    timeout: int,
) -> Dict[str, Any]:
    """Core logic, separated from the FastMCP wrapper for testability."""
    from databricks.sdk.service.sql import Disposition, Format, QueryTag, StatementState

    # Import governance helpers lazily (and from the sibling patch) so this
    # module is importable without the SP environment configured (e.g. tests).
    from customization.sql_executor_patch import (
        get_mcp_user_identity,
        get_sql_sp_client,
        get_sql_warehouse_id,
    )

    fmt = fmt.lower()
    if fmt not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format {fmt!r}. Supported: {', '.join(_SUPPORTED_FORMATS)}.")

    # Clear out files past the retention window before writing a new one.
    sweep_old_exports()

    dest = resolve_export_path(output_path)
    if dest.exists() and not overwrite:
        raise ValueError(f"{dest} already exists. Pass overwrite=true to replace it.")

    warehouse_id = get_sql_warehouse_id()
    identity = get_mcp_user_identity()  # resolve under user creds, before SP client use
    client = get_sql_sp_client()

    query_tags = [
        QueryTag(key="mcp_user", value=identity),
        QueryTag(key="mcp_tool", value="export_query_to_file"),
    ]

    response = client.statement_execution.execute_statement(
        statement=sql_query,
        warehouse_id=warehouse_id,
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.CSV,
        wait_timeout="0s",
        catalog=catalog,
        schema=schema,
        query_tags=query_tags,
    )
    statement_id = response.statement_id

    poll_interval = 2
    elapsed = 0
    status = response
    while True:
        state = status.status.state
        if state == StatementState.SUCCEEDED:
            break
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            msg = ""
            if status.status and status.status.error and status.status.error.message:
                msg = status.status.error.message
            raise RuntimeError(f"Export query did not succeed (state={state}). {msg}".strip())
        if elapsed >= timeout:
            try:
                client.statement_execution.cancel_execution(statement_id=statement_id)
            finally:
                raise RuntimeError(
                    f"Export query timed out after {timeout}s and was canceled "
                    f"(statement_id={statement_id})."
                )
        time.sleep(poll_interval)
        elapsed += poll_interval
        status = client.statement_execution.get_statement(statement_id=statement_id)

    manifest = status.manifest
    columns = (
        [col.name for col in manifest.schema.columns]
        if manifest and manifest.schema and manifest.schema.columns
        else []
    )
    row_count = (manifest.total_row_count if manifest else None) or 0

    links = _collect_external_links(client, statement_id, status.result)
    _write_csv(dest, columns, links)

    return {
        "path": str(dest),
        "row_count": int(row_count),
        "bytes": dest.stat().st_size,
        "columns": columns,
        "format": fmt,
    }


def register_export_query_tool(mcp) -> None:
    """Register ``export_query_to_file`` on the FastMCP server instance.

    Must be called AFTER importing the upstream server module and BEFORE the
    tool allowlist runs (so the allowlist remains the single source of truth
    for what is exposed; ``export_query_to_file`` must be in ALLOWED_TOOLS).

    Args:
        mcp: The FastMCP server instance (from databricks_mcp_server.server).
    """

    @mcp.tool(timeout=EXPORT_TOOL_TIMEOUT_CEILING)
    def export_query_to_file(
        sql_query: str,
        output_path: str,
        format: str = "csv",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        overwrite: bool = False,
        timeout: int = _DEFAULT_STATEMENT_TIMEOUT,
    ) -> Dict[str, Any]:
        """Run a SQL query and write the full result set to a local file.

        Use this instead of execute_sql when the result is large or is destined
        for a downstream tool/model rather than to be read in the conversation.
        The result rows are written to disk by the server and never returned
        through the model context — only a small manifest is returned.

        Args:
            sql_query: The SQL query to run.
            output_path: Destination file, relative to the server's export
                directory (DATABRICKS_MCP_EXPORT_DIR, default
                ~/databricks-mcp-exports). Paths escaping that directory are
                rejected.
            format: Output format. Currently only "csv".
            catalog: Optional catalog context for the query.
            schema: Optional schema context for the query.
            overwrite: Overwrite the destination if it already exists.
            timeout: Per-call statement timeout in seconds (bounded by the
                tool's 600s ceiling).

        Returns:
            A manifest: {path, row_count, bytes, columns, format}. The rows
            themselves are on disk at `path`, not in this response.
        """
        return _run_export(
            sql_query=sql_query,
            output_path=output_path,
            fmt=format,
            catalog=catalog,
            schema=schema,
            overwrite=overwrite,
            timeout=timeout,
        )

    # Sweep stale exports left from previous runs at startup, so retention holds
    # even for an engineer who exports rarely.
    sweep_old_exports()

    logger.info(
        "Registered export_query_to_file tool (ceiling=%ss, export dir=%s, retention=%d days)",
        EXPORT_TOOL_TIMEOUT_CEILING,
        get_export_base_dir(),
        get_export_retention_days(),
    )
