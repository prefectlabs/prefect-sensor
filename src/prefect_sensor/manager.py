"""Run multiple sensors as concurrent asyncio tasks."""

from __future__ import annotations

import asyncio
import logging
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

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> SensorManager:
        sensors = load_sensors_from_yaml(path)
        return cls(sensors)

    @classmethod
    def from_sensors(cls, *sensors: BaseSensor) -> SensorManager:
        return cls(list(sensors))

    async def start(self) -> None:
        if not self._sensors:
            logger.warning("No sensors configured — nothing to run.")
            return

        self._tasks = [
            asyncio.create_task(s.run(), name=f"sensor:{s.config.name}")
            for s in self._sensors
        ]

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

    async def stop(self) -> None:
        logger.info("Stopping all sensors...")
        for s in self._sensors:
            s.request_stop()
        await asyncio.sleep(2)
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def heartbeats(self) -> list[SensorHeartbeat]:
        return [s.heartbeat() for s in self._sensors]

    def summary(self) -> str:
        lines = [f"SensorManager: {len(self._sensors)} sensor(s) configured\n"]
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
