"""Core sensor abstractions: BaseSensor, StatefulSensorMixin."""

from __future__ import annotations

import abc
import asyncio
import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
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
    Mixin for polling-based sensors that track what they've already seen.

    For production, back this with a Prefect Block (JSON, S3, Redis)
    so state survives process restarts.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._seen: set[str] = set()

    def _fingerprint(self, data: Any) -> str:
        raw = json.dumps(data, sort_keys=True, default=str).encode()
        return hashlib.sha256(raw).hexdigest()[:16]

    def _is_new(self, data: Any) -> bool:
        fp = self._fingerprint(data)
        if fp in self._seen:
            return False
        self._seen.add(fp)
        return True


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
