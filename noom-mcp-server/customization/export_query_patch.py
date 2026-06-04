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
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

logger = logging.getLogger(__name__)

# Wall-clock ceiling (seconds) for the export tool call.  Bulk exports of large
# result sets can legitimately run longer than an interactive query, so this
# matches the raised SQL ceiling rather than the upstream 60s default.
EXPORT_TOOL_TIMEOUT_CEILING = 600

# Default per-call statement timeout (seconds) — overridable via the tool's
# ``timeout`` argument, bounded by the ceiling above.
_DEFAULT_STATEMENT_TIMEOUT = 300

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


def _iter_external_links(client, statement_id: str, first_result) -> Iterator[Any]:
    """Yield external links in row order, following the chunk chain lazily.

    Laziness is deliberate. The presigned ``external_link`` URLs are short-lived
    (Databricks issues them per response and they expire — observed ~15 min). If
    we collected every link up front and only then downloaded, a long multi-chunk
    export could outlive the links issued first. By yielding each link as its
    chunk is fetched — and having the caller download it immediately — every URL
    is freshly issued when used, so the at-risk window is one chunk, not the
    whole export.

    Within a single response, links are ordered by ``chunk_index`` (the chunk
    chain itself is ordered via ``next_chunk_index``).
    """
    result = first_result
    while result is not None and result.external_links:
        for link in sorted(
            result.external_links, key=lambda link: (link.chunk_index or 0, link.row_offset or 0)
        ):
            yield link
        nxt = result.next_chunk_index
        if nxt is None:
            break
        result = client.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id, chunk_index=nxt
        )


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


def _write_csv(
    dest: Path,
    columns: List[str],
    links: Iterable[Any],
    overwrite: bool = False,
    base_dir: Optional[Path] = None,
) -> None:
    """Stream each CSV chunk's bytes to ``dest`` in row order, atomically.

    Databricks EXTERNAL_LINKS + CSV chunks already include the column header
    (in the first chunk), so the chunk bytes are written as-is — synthesizing a
    header would duplicate it.

    ``links`` is an iterable, consumed lazily: each link is downloaded as it is
    produced, so when it is the lazy chunk-chain generator
    (``_iter_external_links``) the presigned URL is freshly issued at download
    time rather than minutes earlier.

    The one exception is an empty result: the iterable yields nothing, so a
    header-only CSV is written (derived from the manifest schema) so the file is
    still valid and carries the column names for downstream tools.

    Atomicity: the data is streamed to a temp file in the destination directory
    and moved into place only after the *last* chunk is written. If a chunk
    download fails (or the tool's wall-clock ceiling fires mid-stream), the temp
    file is removed and ``dest`` is left untouched — never a partial or empty
    file at the real path. A half-written CSV that looks complete is exactly the
    silent corruption this tool exists to prevent.

    Overwrite guard: the existence check in the caller runs before a long query,
    so it can't be the authoritative guard (the file may appear while the query
    is in flight). When ``overwrite`` is False the move uses ``os.link``, which
    fails atomically if ``dest`` already exists — closing that TOCTOU window.
    When True it uses ``os.replace`` (atomic clobber).

    Sandbox re-check: ``resolve_export_path`` validated containment before the
    query, but ``dest.parent`` could have become a symlink pointing outside the
    sandbox in the meantime. When ``base_dir`` is given, the destination's parent
    is re-resolved and re-checked for containment immediately before writing, so
    the write can't follow such a symlink out of the export dir.
    """
    if base_dir is not None:
        base = base_dir.resolve()
        real_parent = dest.parent.resolve()
        if real_parent != base and base not in real_parent.parents:
            raise ValueError(
                f"refusing to write {dest}: its directory resolves outside the export "
                f"sandbox ({base}) — a symlink may have been introduced after the path "
                f"was validated."
            )

    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".part")
    tmp = Path(tmp_name)
    try:
        wrote_any = False
        with os.fdopen(fd, "wb") as fh:
            for link in links:
                fh.write(_download_link(link))
                wrote_any = True
            if not wrote_any:
                fh.write(_csv_header_line(columns).encode("utf-8"))
        if overwrite:
            os.replace(tmp, dest)  # atomic clobber, same filesystem
        else:
            # Atomic no-clobber: fails if dest exists, even if it appeared during
            # the query (after the caller's early existence check).
            try:
                os.link(tmp, dest)
            except FileExistsError as exc:
                raise ValueError(
                    f"{dest} already exists. Pass overwrite=true to replace it."
                ) from exc
        tmp.unlink(missing_ok=True)  # for the os.link path; no-op after os.replace
    except BaseException:
        # Includes timeouts/cancellation: never leave a partial file behind,
        # and never clobber a prior good export at `dest`.
        tmp.unlink(missing_ok=True)
        raise


def _csv_header_line(columns: List[str]) -> str:
    import io

    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(columns)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------


def _clamp_statement_timeout(timeout: int) -> int:
    """Bound the per-call statement timeout by the tool's wall-clock ceiling.

    A caller-supplied timeout above ``EXPORT_TOOL_TIMEOUT_CEILING`` would let
    FastMCP abort the tool call before the poll loop's own cancellation fires,
    leaving the statement running on the warehouse. Clamping keeps our cancel
    within the ceiling. Logs when a value is reduced.
    """
    if timeout > EXPORT_TOOL_TIMEOUT_CEILING:
        logger.warning(
            "export timeout %ss exceeds the %ss ceiling; clamping.",
            timeout,
            EXPORT_TOOL_TIMEOUT_CEILING,
        )
        return EXPORT_TOOL_TIMEOUT_CEILING
    return timeout


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

    # Clamp the per-call timeout to the tool's wall-clock ceiling (see
    # _clamp_statement_timeout): a value above the ceiling would let FastMCP
    # abort the tool call before our own cancellation fires, orphaning the
    # statement on the warehouse. The poll loop below cancels at this bound.
    timeout = _clamp_statement_timeout(timeout)

    base = get_export_base_dir()
    dest = resolve_export_path(output_path, base)
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

    # Lazy generator: each chunk's presigned link is fetched and downloaded
    # just-in-time (see _iter_external_links / _write_csv), so links don't age
    # out during a long multi-chunk download.
    links = _iter_external_links(client, statement_id, status.result)
    _write_csv(dest, columns, links, overwrite=overwrite, base_dir=base)

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

    logger.info(
        "Registered export_query_to_file tool (ceiling=%ss, export dir=%s)",
        EXPORT_TOOL_TIMEOUT_CEILING,
        get_export_base_dir(),
    )
