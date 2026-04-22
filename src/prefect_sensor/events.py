"""Async-friendly wrapper around Prefect's synchronous event emission."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from prefect.events import emit_event as prefect_emit_event


async def emit_event_async(
    event: str,
    resource: dict[str, str],
    payload: dict[str, Any] | None = None,
    occurred: datetime | None = None,
    related: list[dict[str, str]] | None = None,
) -> Any:
    """Emit a Prefect event without blocking the asyncio event loop."""
    return await asyncio.to_thread(
        prefect_emit_event,
        event,
        resource,
        occurred=occurred,
        related=related,
        payload=payload,
    )
