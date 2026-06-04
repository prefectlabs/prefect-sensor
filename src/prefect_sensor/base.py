"""Core sensor abstractions: BaseSensor, StatefulSensorMixin."""

from __future__ import annotations

import abc
import asyncio
import json
import logging
import os
import tempfile
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prefect_sensor._internal.schema import (
    SensorHeartbeat,
    SensorObservation,
    SensorState,
)
from prefect_sensor._internal.schema.config import SensorConfig
from prefect_sensor.events import emit_event_async

logger = logging.getLogger("prefect.sensors")


class StatefulSensorMixin:
    """
    Mixin that persists a single JSON-serializable state value to disk.

    Subclasses call ``_load_state()`` during ``setup()`` to seed their
    in-memory state, and ``_save_state(value)`` after material updates.
    Writes are atomic via tempfile + ``os.replace``.

    Storage format is ``{"state": <value>}``. For backward compatibility
    the loader also accepts the legacy ``{"hwm": <value>}`` shape used by
    older SQL state files.

    ``datetime`` values are serialized with ``isoformat()``. Subclasses
    that need typed deserialization (e.g. parsing ISO strings back to
    ``datetime``) should override ``_deserialize_state``.
    """

    def _state_path(self) -> Path | None:
        state_file = getattr(self.config, "state_file", None)
        if not state_file:
            return None
        return Path(state_file).expanduser()

    def _serialize_state(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _deserialize_state(self, value: Any) -> Any:
        return value

    def _load_state(self) -> Any:
        path = self._state_path()
        if path is None or not path.exists():
            return None
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            self.logger.warning("Corrupt state file %s: %s", path, exc)
            return None
        if not isinstance(raw, dict):
            self.logger.warning("Unexpected state file shape in %s: %r", path, raw)
            return None
        if "state" in raw:
            value = raw["state"]
        elif "hwm" in raw:
            value = raw["hwm"]
        else:
            return None
        if value is None:
            return None
        return self._deserialize_state(value)

    def _save_state(self, value: Any) -> None:
        path = self._state_path()
        if path is None or value is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)

        serialized = {"state": self._serialize_state(value)}

        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(serialized, f, default=str)
            os.replace(tmp_name, path)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise


class BaseSensor(abc.ABC):
    """
    Abstract base for all sensors.

    Lifecycle:  setup()  →  observe() loop  →  teardown()

    Subclasses MUST implement:
        observe() — async generator yielding SensorObservation objects

    Subclasses MUST define:
        Config — a nested SensorConfig subclass (or set _config_class)

    Subclasses MAY override:
        setup() / teardown() for connection management
        build_event_name() / build_resource() to customize event shape
    """

    _config_class: type[SensorConfig] = SensorConfig

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        nested = getattr(cls, "Config", None)
        if (
            nested is not None
            and isinstance(nested, type)
            and issubclass(nested, SensorConfig)
        ):
            cls._config_class = nested

    def __init__(self, config: SensorConfig) -> None:
        self.config = config
        self.state = SensorState.IDLE
        self.logger = logging.getLogger(f"sensor.{config.name}")
        self._events_emitted: int = 0
        self._errors: int = 0
        self._consecutive_errors: int = 0
        self._last_event_at: datetime | None = None
        self._last_error: str | None = None
        self._started_at: float | None = None
        self._stop_event = asyncio.Event()

    async def setup(self) -> None:
        """Open connections, initialize clients. Called once before observe loop."""

    async def teardown(self) -> None:
        """Close connections, flush buffers. Called once after observe loop."""

    @abc.abstractmethod
    async def observe(self) -> AsyncIterator[SensorObservation]:
        yield  # type: ignore[misc]

    def _type_name(self) -> str:
        return self.__class__.__name__

    def build_event_name(self, obs: SensorObservation) -> str:
        return f"{self.config.emit_prefix}.{obs.event_type}"

    def build_resource(self, obs: SensorObservation) -> dict[str, str]:
        return {
            "prefect.resource.id": obs.resource_id,
            "prefect.resource.role": "sensor-target",
            "sensor.type": self._type_name(),
            "sensor.name": self.config.name,
        }

    def build_related(self, obs: SensorObservation) -> list[dict[str, str]]:
        return [
            {
                "prefect.resource.id": f"prefect.sensor.{self.config.name}",
                "prefect.resource.role": "sensor",
            }
        ]

    async def run(self) -> None:
        self.state = SensorState.STARTING
        self._started_at = time.monotonic()
        try:
            await self.setup()
            self.state = SensorState.RUNNING
            self.logger.info(
                "Sensor '%s' (%s) started.",
                self.config.name,
                self._type_name(),
            )

            while not self._stop_event.is_set():
                try:
                    async for obs in self.observe():
                        if self._stop_event.is_set():
                            break
                        await self._emit(obs)
                        self._consecutive_errors = 0
                except Exception as exc:  # noqa: BLE001 — sensor resilience
                    self._errors += 1
                    self._consecutive_errors += 1
                    self._last_error = str(exc)
                    self.logger.error(
                        "observe error (%d/%d): %s",
                        self._consecutive_errors,
                        self.config.max_consecutive_errors,
                        exc,
                    )
                    if self._consecutive_errors >= self.config.max_consecutive_errors:
                        self.state = SensorState.ERRORED
                        self.logger.critical("Max consecutive errors — halting.")
                        break
                    await asyncio.sleep(self.config.error_backoff_seconds)
        finally:
            self.state = SensorState.STOPPING
            await self.teardown()
            self.state = SensorState.IDLE
            self.logger.info("Sensor '%s' stopped.", self.config.name)

    def request_stop(self) -> None:
        self._stop_event.set()

    async def _emit(self, obs: SensorObservation) -> None:
        now = datetime.now(timezone.utc)
        await emit_event_async(
            event=self.build_event_name(obs),
            resource=self.build_resource(obs),
            payload=obs.payload,
            occurred=obs.occurred or now,
            related=self.build_related(obs),
        )
        self._events_emitted += 1
        self._last_event_at = now

    def heartbeat(self) -> SensorHeartbeat:
        return SensorHeartbeat(
            sensor_name=self.config.name,
            sensor_type=self._type_name(),
            state=self.state,
            events_emitted=self._events_emitted,
            errors=self._errors,
            last_event_at=self._last_event_at,
            last_error=self._last_error,
            uptime_seconds=(
                time.monotonic() - self._started_at if self._started_at else 0.0
            ),
        )
