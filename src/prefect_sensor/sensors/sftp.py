"""SFTP remote directory sensor backed by Paramiko."""

from __future__ import annotations

import asyncio
import posixpath
import stat as statmod
from collections.abc import AsyncIterator
from typing import Any, Dict, List, Optional

import paramiko
from pydantic import Field

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class SFTPSensor(StatefulSensorMixin, BaseSensor):
    """Poll remote directories over SFTP and emit file lifecycle events."""

    class Config(SensorConfig):
        hostname: str
        port: int = 22
        username: str = ""
        password: str = ""
        private_key_path: str = ""
        allow_agent: bool = True
        look_for_keys: bool = True
        remote_directories: List[str] = Field(default_factory=lambda: ["/upload"])
        poll_interval_seconds: float = 30.0
        emit_prefix: str = "sensor.sftp"

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._known: Dict[str, Dict[str, Any]] = {}
        self._last_mtime: int = 0
        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._sftp_client: Optional[paramiko.SFTPClient] = None

    async def setup(self) -> None:
        loaded = self._load_state()
        self._last_mtime = int(loaded) if loaded is not None else 0
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs: dict[str, Any] = {
            "hostname": self.config.hostname,
            "port": self.config.port,
            "username": self.config.username or None,
            "password": self.config.password or None,
            "allow_agent": self.config.allow_agent,
            "look_for_keys": self.config.look_for_keys,
        }
        if self.config.private_key_path:
            connect_kwargs["key_filename"] = self.config.private_key_path
        self._ssh_client.connect(**connect_kwargs)
        self._sftp_client = self._ssh_client.open_sftp()
        self.logger.info(
            "SFTP connected to %s:%d",
            self.config.hostname,
            self.config.port,
        )

    async def teardown(self) -> None:
        self._save_state(self._last_mtime)
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None
        if self._ssh_client is not None:
            self._ssh_client.close()
            self._ssh_client = None
        self.logger.info("SFTP disconnected.")

    async def observe(self) -> AsyncIterator[SensorObservation]:
        if self._sftp_client is None:
            raise RuntimeError("SFTPSensor.setup() must be called before observe().")

        current_keys: set[str] = set()
        for rdir in self.config.remote_directories:
            try:
                filenames = self._sftp_client.listdir(rdir)
            except FileNotFoundError:
                self.logger.warning("Remote directory missing: %s", rdir)
                continue

            for filename in filenames:
                remote_path = posixpath.join(rdir, filename)
                stat_result = self._sftp_client.stat(remote_path)
                if not statmod.S_ISREG(stat_result.st_mode):
                    continue
                meta = {"size": stat_result.st_size, "mtime": int(stat_result.st_mtime)}
                current_keys.add(remote_path)
                prev = self._known.get(remote_path)

                if prev is None:
                    self._known[remote_path] = meta
                    if meta["mtime"] <= self._last_mtime:
                        continue
                    self._last_mtime = max(self._last_mtime, meta["mtime"])
                    yield SensorObservation(
                        event_type="file.appeared",
                        resource_id=f"sftp:{self.config.hostname}:{remote_path}",
                        payload={
                            "hostname": self.config.hostname,
                            "remote_path": remote_path,
                            **meta,
                        },
                    )
                elif prev != meta:
                    self._known[remote_path] = meta
                    self._last_mtime = max(self._last_mtime, meta["mtime"])
                    yield SensorObservation(
                        event_type="file.changed",
                        resource_id=f"sftp:{self.config.hostname}:{remote_path}",
                        payload={
                            "hostname": self.config.hostname,
                            "remote_path": remote_path,
                            **meta,
                        },
                    )
                self._known[remote_path] = meta

        for gone in set(self._known) - current_keys:
            del self._known[gone]
            yield SensorObservation(
                event_type="file.removed",
                resource_id=f"sftp:{self.config.hostname}:{gone}",
                payload={
                    "hostname": self.config.hostname,
                    "remote_path": gone,
                },
            )

        self._save_state(self._last_mtime)
        await asyncio.sleep(self.config.poll_interval_seconds)
