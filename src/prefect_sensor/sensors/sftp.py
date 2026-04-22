"""SFTP remote directory sensor (stub — use asyncssh in production)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Dict, List

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class SFTPSensor(StatefulSensorMixin, BaseSensor):
    """Stub SFTP poller that emits synthetic directory listings."""

    class Config(SensorConfig):
        hostname: str
        port: int = 22
        username: str = ""
        password: str = ""
        private_key_path: str = ""
        remote_directories: List[str] = ["/upload"]
        poll_interval_seconds: float = 30.0
        emit_prefix: str = "sensor.sftp"

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._known: Dict[str, Dict[str, Any]] = {}

    async def setup(self) -> None:
        self.logger.info(
            "SFTP connected to %s:%d",
            self.config.hostname,
            self.config.port,
        )

    async def teardown(self) -> None:
        self.logger.info("SFTP disconnected.")

    async def observe(self) -> AsyncIterator[SensorObservation]:
        for rdir in self.config.remote_directories:
            entries = [
                {"filename": "report.csv", "size": 1048576, "mtime": 1706000000},
            ]

            current_keys: set[str] = set()
            for entry in entries:
                key = f"{rdir}/{entry['filename']}"
                current_keys.add(key)
                meta = {"size": entry["size"], "mtime": entry["mtime"]}
                prev = self._known.get(key)

                if prev is None:
                    yield SensorObservation(
                        event_type="file.appeared",
                        resource_id=f"sftp:{self.config.hostname}:{key}",
                        payload={
                            "hostname": self.config.hostname,
                            "remote_path": key,
                            **meta,
                        },
                    )
                elif prev != meta:
                    yield SensorObservation(
                        event_type="file.changed",
                        resource_id=f"sftp:{self.config.hostname}:{key}",
                        payload={
                            "hostname": self.config.hostname,
                            "remote_path": key,
                            **meta,
                        },
                    )
                self._known[key] = meta

            for gone in set(self._known) - current_keys:
                if gone.startswith(rdir):
                    yield SensorObservation(
                        event_type="file.removed",
                        resource_id=f"sftp:{self.config.hostname}:{gone}",
                        payload={
                            "hostname": self.config.hostname,
                            "remote_path": gone,
                        },
                    )
                    del self._known[gone]

        await asyncio.sleep(self.config.poll_interval_seconds)
