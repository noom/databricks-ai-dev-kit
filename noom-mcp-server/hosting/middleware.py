"""ASGI middleware for the hosted MCP server.

IdentityMiddleware runs before every HTTP request. It extracts the
X-Forwarded-User header (set by the Databricks Apps OAuth proxy) and
populates the per-request ContextVar so sql_executor_patch can tag every
SQL statement with the calling user's identity.

Non-HTTP ASGI scopes (lifespan, websocket) pass through unchanged so that
the FastMCP session manager starts and shuts down correctly.
"""

import logging

from hosting.request_identity import set_user_from_request

logger = logging.getLogger(__name__)


class IdentityMiddleware:
    """ASGI middleware: extract X-Forwarded-User and set per-request identity."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            headers = {
                k.decode("latin-1"): v.decode("latin-1") for k, v in scope.get("headers", [])
            }
            set_user_from_request(headers)
        await self.app(scope, receive, send)
