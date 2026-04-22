"""Filesystem polling sensor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Dict, List

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class FileSystemSensor(StatefulSensorMixin, BaseSensor):
    """Poll watch paths for file create / modify / delete."""

    class Config(SensorConfig):
        watch_paths: List[str]
        patterns: List[str] = ["*"]
        recursive: bool = True
        poll_interval_seconds: float = 2.0
        emit_prefix: str = "sensor.filesystem"

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._previous: Dict[str, float] = {}

    async def observe(self) -> AsyncIterator[SensorObservation]:
        current: Dict[str, float] = {}

        for wp in self.config.watch_paths:
            p = Path(wp)
            if not p.exists():
                continue
            glb = p.rglob if self.config.recursive else p.glob
            for pat in self.config.patterns:
                for fp in glb(pat):
                    if fp.is_file():
                        try:
                            current[str(fp)] = fp.stat().st_mtime
                        except OSError:
                            pass

        for fpath, mtime in current.items():
            prev = self._previous.get(fpath)
            if prev is None:
                yield SensorObservation(
                    event_type="file.created",
                    resource_id=f"filesystem:{fpath}",
                    payload={"path": fpath, "mtime": mtime},
                )
            elif mtime != prev:
                yield SensorObservation(
                    event_type="file.modified",
                    resource_id=f"filesystem:{fpath}",
                    payload={
                        "path": fpath,
                        "mtime": mtime,
                        "prev_mtime": prev,
                    },
                )

        for fpath in set(self._previous) - set(current):
            yield SensorObservation(
                event_type="file.deleted",
                resource_id=f"filesystem:{fpath}",
                payload={"path": fpath},
            )

        self._previous = current
        await asyncio.sleep(self.config.poll_interval_seconds)
