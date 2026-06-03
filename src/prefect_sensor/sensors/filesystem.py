"""Watchdog-backed filesystem sensor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from watchdog.events import (
    FileSystemEvent,
    PatternMatchingEventHandler,
    EVENT_TYPE_MOVED,
)
from watchdog.observers import Observer

from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig
from prefect_sensor.base import BaseSensor, StatefulSensorMixin

_SUPPORTED_EVENTS = {"created", "modified", "deleted", "moved"}
_DEFAULT_EVENTS = ["created", "modified", "deleted", "moved"]


class FileSystemSensor(StatefulSensorMixin, BaseSensor):
    """Watch configured paths for file and directory changes."""

    class Config(SensorConfig):
        watch_paths: list[str]

        recursive: bool = True
        events: list[str] = Field(default_factory=lambda: _DEFAULT_EVENTS.copy())
        emit_prefix: str = "sensor.filesystem"

        patterns: list[str] = Field(default_factory=lambda: ["*"])
        ignore_patterns: list[str] = Field(default=None)
        ignore_directories: bool = True
        case_sensitive: bool = False

        @field_validator("events")
        @classmethod
        def _validate_events(cls, values: list[str]) -> list[str]:
            normalized: list[str] = []
            seen: set[str] = set()
            for value in values:
                event = value.strip().lower()
                if event not in _SUPPORTED_EVENTS:
                    msg = (
                        f"Unsupported filesystem event '{value}'. "
                        f"Supported events: {sorted(_SUPPORTED_EVENTS)}"
                    )
                    raise ValueError(msg)
                if event not in seen:
                    normalized.append(event)
                    seen.add(event)
            return normalized

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._observer: Observer | None = None
        self._handler: _WatchdogEventHandler | None = None
        self._queue: asyncio.Queue[SensorObservation | None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._watch_roots: list[Path] = []

    async def setup(self) -> None:
        self._queue = asyncio.Queue()
        self._handler = _WatchdogEventHandler(
            self,
            patterns=self.config.patterns,
            ignore_patterns=self.config.ignore_patterns,
            ignore_directories=self.config.ignore_directories,
            case_sensitive=self.config.case_sensitive,
        )
        self._watch_roots = []

        for wp in self.config.watch_paths:
            root = Path(wp).expanduser().absolute()
            if not root.exists():
                self.logger.warning("Skipping missing watch path: %s", root)
                continue
            self._watch_roots.append(root)

        if not self._watch_roots:
            self.logger.warning(
                "No existing watch paths configured; sensor will idle until stopped."
            )
            return

        self._observer = Observer()
        for root in self._watch_roots:
            self._observer.schedule(
                self._handler,
                str(root),
                recursive=self.config.recursive,
            )
        self._observer.start()

    async def observe(self) -> AsyncIterator[SensorObservation]:
        if self._queue is None:
            msg = "setup() must be called before observe()."
            raise RuntimeError(msg)

        self._loop = asyncio.get_running_loop()
        try:
            while not self._stop_event.is_set():
                obs = await self._queue.get()
                if obs is None:
                    break
                yield obs
        finally:
            self._loop = None

    async def teardown(self) -> None:
        observer = self._observer
        self._observer = None
        self._handler = None

        self._signal_stop()

        if observer is not None:
            observer.stop()
            await asyncio.to_thread(observer.join)

        self._queue = None
        self._watch_roots = []

    def request_stop(self) -> None:
        super().request_stop()
        self._signal_stop()

    def _signal_stop(self) -> None:
        if self._loop is None or self._queue is None:
            return
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, None)

    def _candidate_root(self, path: Path) -> tuple[Path, Path] | None:
        for root in self._watch_roots:
            try:
                rel = path.relative_to(root)
            except ValueError:
                continue
            if not self.config.recursive and len(rel.parts) > 1:
                continue
            return root, rel
        return None

    def _queue_observation(self, obs: SensorObservation) -> None:
        if self._loop is None or self._queue is None:
            return
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, obs)

    def _handle_watchdog_event(self, event_name: str, event: FileSystemEvent) -> None:
        if event_name not in self.config.events:
            return

        path = event.dest_path or event.src_path
        root, _ = self._candidate_root(Path(str(path))) or (None, None)
        kind = "directory" if event.is_directory else "file"
        payload: dict[str, Any] = {
            "path": str(path),
            "kind": kind,
            "is_directory": event.is_directory,
            "watch_root": str(root),
        }

        if event.event_type == EVENT_TYPE_MOVED:
            payload["source_path"] = str(event.src_path)
            payload["destination_path"] = str(getattr(event, "dest_path", path))

        self._queue_observation(
            SensorObservation(
                event_type=f"{kind}.{event_name}",
                resource_id=f"filesystem:{path}",
                payload=payload,
            )
        )


class _WatchdogEventHandler(PatternMatchingEventHandler):
    def __init__(self, sensor: FileSystemSensor, **kwargs) -> None:
        super().__init__(**kwargs)
        self._sensor = sensor

    def on_created(self, event: FileSystemEvent) -> None:
        self._sensor._handle_watchdog_event("created", event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._sensor._handle_watchdog_event("modified", event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self._sensor._handle_watchdog_event("deleted", event)

    def on_moved(self, event: FileSystemEvent) -> None:
        self._sensor._handle_watchdog_event("moved", event)
