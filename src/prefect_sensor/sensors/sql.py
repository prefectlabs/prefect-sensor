"""SQL polling sensor (stub — use SQLAlchemy async in production)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from prefect_sensor.base import BaseSensor
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class SQLSensor(BaseSensor):
    """Stub SQL sensor that emits synthetic rows and tracks a high-water mark."""

    class Config(SensorConfig):
        connection_string: str = ""
        query: str = ""
        tracking_column: str = "id"
        poll_interval_seconds: float = 15.0
        emit_prefix: str = "sensor.sql"

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._hwm: Any = None

    async def setup(self) -> None:
        self.logger.info(
            "SQL sensor ready, tracking '%s'",
            self.config.tracking_column,
        )

    async def observe(self) -> AsyncIterator[SensorObservation]:
        rows = [
            {
                "id": 1001,
                "customer": "acme",
                "status": "new",
                "amount": 250.0,
            },
        ]

        for row in rows:
            val = row.get(self.config.tracking_column)
            yield SensorObservation(
                event_type="row.detected",
                resource_id=f"sql:{self.config.tracking_column}:{val}",
                payload=dict(row),
            )
            if self._hwm is None or val > self._hwm:
                self._hwm = val

        await asyncio.sleep(self.config.poll_interval_seconds)
