"""SQL polling sensor that tracks a monotonically rising column."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

from pydantic import Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig
from prefect_sensor.base import BaseSensor, StatefulSensorMixin


_START_VALUE_GLOBALS: dict[str, Any] = {
    "__builtins__": {},
    "datetime": datetime,
    "timezone": timezone,
    "timedelta": timedelta,
}


def _eval_start_value(expr: str) -> Any:
    return eval(expr, _START_VALUE_GLOBALS, {})  # noqa: S307 — user-controlled config


class SQLSensor(StatefulSensorMixin, BaseSensor):
    """Poll a SQL table for new rows by tracking a monotonically rising column.

    The sensor runs ``query`` on each poll, binding the current high-water-mark
    (HWM) into the ``:hwm`` parameter. For every returned row it emits one
    ``SensorObservation`` and advances the HWM to the value of the row's
    ``tracking_column``.

    The query must include the ``:hwm`` bind parameter. A canonical shape is::

        SELECT id, customer, status, amount
        FROM orders
        WHERE id > :hwm
        ORDER BY id

    The ``tracking_column`` (here ``id``) must appear in the SELECT list and must
    be monotonically non-decreasing in the ordering produced by the query.

    Two tracking types are supported:

    - ``integer`` — any numeric column (IDs, counters).
    - ``timestamp`` — ``datetime`` column. Naive values returned by the driver are
      coerced to UTC. Stored on disk as ISO 8601 with timezone.

    The initial HWM is resolved in this order on ``setup()``:

    1. If ``state_file`` is set and the file exists, the HWM is loaded from it.
    2. Otherwise, if ``start_value`` is provided, it is ``eval()``-ed against a
       restricted namespace exposing ``datetime``, ``timezone``, ``timedelta``.
       Examples: ``"1000"``, ``"datetime(2024, 1, 1, tzinfo=timezone.utc)"``.
    3. Otherwise, if ``emit_existing=True``, the HWM starts at a type-appropriate
       floor so the first poll emits every row currently in the table:
       ``0`` for ``integer``, ``datetime.min`` (UTC) for ``timestamp``.

    Exactly one of ``start_value`` or ``emit_existing`` must be configured (the
    state file may override on subsequent runs).

    When ``state_file`` is set, the HWM is written atomically after each poll so
    sensor restarts resume without replaying already-emitted rows.

    Example YAML::

        sensors:
          - prefect_sensor.sensors.sql.SQLSensor:
              name: orders-watcher
              connection_string: "sqlite+aiosqlite:///./orders.db"
              query: "SELECT id, customer, status FROM orders WHERE id > :hwm ORDER BY id"
              tracking_column: id
              tracking_type: integer
              emit_existing: true
              state_file: /var/lib/prefect-sensor/orders.json
              poll_interval_seconds: 5
    """

    class Config(SensorConfig):
        connection_string: str
        query: str
        tracking_column: str
        tracking_type: Literal["integer", "timestamp"] = "integer"
        start_value: Optional[str] = None
        emit_existing: bool = False
        poll_interval_seconds: float = Field(default=15.0, ge=0)
        emit_prefix: str = "sensor.sql"

        @model_validator(mode="after")
        def _validate_query_and_start(self) -> "SQLSensor.Config":
            if ":hwm" not in self.query:
                msg = (
                    "SQLSensor.query must contain the ':hwm' bind parameter "
                    "(e.g. 'WHERE id > :hwm')."
                )
                raise ValueError(msg)

            if self.start_value is not None and self.emit_existing:
                msg = "Set exactly one of start_value or emit_existing, not both."
                raise ValueError(msg)
            if self.start_value is None and not self.emit_existing:
                msg = (
                    "SQLSensor requires either start_value or emit_existing=true "
                    "to seed the initial HWM (unless a state_file already exists)."
                )
                raise ValueError(msg)

            if self.start_value is not None:
                try:
                    _eval_start_value(self.start_value)
                except Exception as exc:
                    msg = f"Could not evaluate start_value {self.start_value!r}: {exc}"
                    raise ValueError(msg) from exc

            return self

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._engine: AsyncEngine | None = None
        self._hwm: Any = None

    async def setup(self) -> None:
        self._engine = create_async_engine(self.config.connection_string)
        self._hwm = self._resolve_initial_hwm()
        self.logger.info(
            "SQL sensor ready: tracking '%s' (%s), initial hwm=%r",
            self.config.tracking_column,
            self.config.tracking_type,
            self._hwm,
        )

    async def teardown(self) -> None:
        self._save_state(self._hwm)
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    async def observe(self) -> AsyncIterator[SensorObservation]:
        if self._engine is None:
            msg = "SQLSensor.setup() must be called before observe()."
            raise RuntimeError(msg)

        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(self.config.query),
                {"hwm": self._serialize_for_bind(self._hwm)},
            )
            rows = result.mappings().all()

        for row in rows:
            raw_val = row[self.config.tracking_column]
            val = self._coerce_tracking(raw_val)
            payload = dict(row)
            payload[self.config.tracking_column] = val
            yield SensorObservation(
                event_type="row.detected",
                resource_id=f"sql:{self.config.tracking_column}:{val}",
                payload=payload,
            )
            self._hwm = val

        if rows:
            self._save_state(self._hwm)

        await asyncio.sleep(self.config.poll_interval_seconds)

    def _resolve_initial_hwm(self) -> Any:
        from_state = self._load_state()
        if from_state is not None:
            return from_state
        if self.config.start_value is not None:
            return _eval_start_value(self.config.start_value)
        if self.config.tracking_type == "timestamp":
            return datetime.min.replace(tzinfo=timezone.utc)
        return 0

    def _coerce_tracking(self, value: Any) -> Any:
        if self.config.tracking_type != "timestamp":
            return value
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _serialize_for_bind(self, value: Any) -> Any:
        if self.config.tracking_type == "timestamp" and isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
        return value

    def _deserialize_state(self, value: Any) -> Any:
        if self.config.tracking_type == "timestamp" and isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        return value
