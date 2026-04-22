"""Run multiple sensors as concurrent asyncio tasks."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from typing import Union

from prefect_sensor.base import BaseSensor
from prefect_sensor._internal.schema import SensorHeartbeat
from prefect_sensor.loader import load_sensors_from_yaml

logger = logging.getLogger("prefect.sensors")


class SensorManager:
    """
    Runs all sensors from a YAML config as concurrent async tasks
    within a single process.

    Usage::

        manager = SensorManager.from_yaml("sensor.yaml")
        await manager.start()
    """

    def __init__(self, sensors: list[BaseSensor]) -> None:
        self._sensors = sensors
        self._tasks: list[asyncio.Task[None]] = []
        self._summary_task: asyncio.Task[None] | None = None

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> SensorManager:
        sensors = load_sensors_from_yaml(path)
        return cls(sensors)

    @classmethod
    def from_sensors(cls, *sensors: BaseSensor) -> SensorManager:
        return cls(list(sensors))

    async def start(self, summary_interval_seconds: float = 60.0) -> None:
        if summary_interval_seconds <= 0:
            msg = "summary_interval_seconds must be greater than 0"
            raise ValueError(msg)
        if not self._sensors:
            logger.warning("No sensors configured — nothing to run.")
            return

        self._tasks = [
            asyncio.create_task(s.run(), name=f"sensor:{s.config.name}")
            for s in self._sensors
        ]

        self._summary_task = asyncio.create_task(
            self._log_summary_periodically(summary_interval_seconds),
            name="sensor:summary",
        )

        try:
            done, _pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc:
                    logger.error(
                        "Sensor task '%s' crashed: %s",
                        task.get_name(),
                        exc,
                    )
        finally:
            if self._summary_task is not None:
                self._summary_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._summary_task
                self._summary_task = None

    async def stop(self) -> None:
        logger.info("Stopping all sensors...")
        if self._summary_task is not None:
            self._summary_task.cancel()
        for s in self._sensors:
            s.request_stop()
        await asyncio.sleep(2)
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._summary_task is not None:
            with suppress(asyncio.CancelledError):
                await self._summary_task
            self._summary_task = None

    async def _log_summary_periodically(self, interval_seconds: float) -> None:
        while any(not task.done() for task in self._tasks):
            await asyncio.sleep(interval_seconds)
            if any(not task.done() for task in self._tasks):
                logger.info("%s", self.summary())

    def heartbeats(self) -> list[SensorHeartbeat]:
        return [s.heartbeat() for s in self._sensors]

    def summary(self) -> str:
        lines = [f"SensorManager: {len(self._sensors)} sensor(s) configured"]
        for s in self._sensors:
            hb = s.heartbeat()
            lines.append(
                f"  • {hb.sensor_name:30s} │ {hb.sensor_type:25s} │ "
                f"{hb.state.value:8s} │ {hb.events_emitted} events"
            )
        return "\n".join(lines)

    @property
    def sensors(self) -> list[BaseSensor]:
        """Configured sensors (read-only use)."""
        return self._sensors
