"""Per-request user identity for the hosted MCP server.

In local mode, identity is resolved once at startup from the OAuth cache
(process-scoped: one user per process).

In hosted mode, many users share one process. Identity arrives per-request
via the X-Forwarded-User header injected by the Databricks Apps auth proxy.
A ContextVar holds the current request's identity so sql_executor_patch can
read it without any cross-request leakage.

Usage:
    # In middleware, before the request handler runs:
    set_user_from_request(headers)

    # In sql_executor_patch.get_mcp_user_identity():
    return get_current_mcp_user()
"""

import contextvars
import logging

logger = logging.getLogger(__name__)

_current_user: contextvars.ContextVar[str] = contextvars.ContextVar("mcp_user", default="unknown")


def set_user_from_request(headers: dict) -> None:
    """Extract X-Forwarded-User from headers and store in the ContextVar.

    Args:
        headers: Dict of lowercase header names → values (as decoded strings).
    """
    email = headers.get("x-forwarded-user", "unknown")
    _current_user.set(email)
    logger.debug("Request identity: %s", email)


def get_current_mcp_user() -> str:
    """Return the identity set for the current request context.

    Returns 'unknown' if called outside a request context or if
    X-Forwarded-User was absent from the request headers.
    """
    return _current_user.get()
