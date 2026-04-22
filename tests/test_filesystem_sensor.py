"""Tests for FileSystemSensor."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from prefect_sensor.sensors.filesystem import FileSystemSensor


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            msg = "timed out waiting for filesystem event"
            raise AssertionError(msg)
        await asyncio.sleep(0.05)


async def _start_sensor(sensor: FileSystemSensor) -> asyncio.Task[None]:
    task = asyncio.create_task(sensor.run())
    await _wait_until(lambda: sensor.state.value == "running")
    return task


async def _stop_sensor(sensor: FileSystemSensor, task: asyncio.Task[None]) -> None:
    sensor.request_stop()
    await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_filesystem_created_modified_deleted(tmp_path: Path) -> None:
    watch = tmp_path / "w"
    watch.mkdir()
    cfg = FileSystemSensor.Config(
        name="fs-test",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["created", "modified", "deleted"],
    )
    sensor = FileSystemSensor(cfg)
    emitted: list[dict[str, object]] = []

    async def capture(*, event, resource, payload, occurred=None, related=None):
        emitted.append({"event": event, "resource": resource, "payload": payload})

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            f = watch / "a.txt"
            f.write_text("hello", encoding="utf-8")
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.file.created"
                    for event in emitted
                )
            )

            f.write_text("world", encoding="utf-8")
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.file.modified"
                    for event in emitted
                )
            )

            f.unlink()
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.file.deleted"
                    for event in emitted
                )
            )
        finally:
            await _stop_sensor(sensor, task)

    event_types = [event["event"] for event in emitted]
    assert "sensor.filesystem.file.created" in event_types
    assert "sensor.filesystem.file.modified" in event_types
    assert "sensor.filesystem.file.deleted" in event_types
    assert sensor.state.value == "idle"


@pytest.mark.asyncio
async def test_filesystem_allowlist_filters_events(tmp_path: Path) -> None:
    watch = tmp_path / "w"
    watch.mkdir()
    cfg = FileSystemSensor.Config(
        name="fs-allowlist",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["created"],
    )
    sensor = FileSystemSensor(cfg)
    emitted: list[dict[str, object]] = []

    async def capture(*, event, resource, payload, occurred=None, related=None):
        emitted.append({"event": event, "resource": resource, "payload": payload})

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            f = watch / "b.txt"
            f.write_text("hello", encoding="utf-8")
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.file.created"
                    for event in emitted
                )
            )

            f.write_text("world", encoding="utf-8")
            f.unlink()
            await asyncio.sleep(0.25)
        finally:
            await _stop_sensor(sensor, task)

    assert [event["event"] for event in emitted] == ["sensor.filesystem.file.created"]


@pytest.mark.asyncio
async def test_filesystem_moved_event(tmp_path: Path) -> None:
    watch = tmp_path / "w"
    watch.mkdir()
    src = watch / "move-me.txt"
    src.write_text("hello", encoding="utf-8")

    cfg = FileSystemSensor.Config(
        name="fs-moved",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["moved"],
    )
    sensor = FileSystemSensor(cfg)
    emitted: list[dict[str, object]] = []

    async def capture(*, event, resource, payload, occurred=None, related=None):
        emitted.append({"event": event, "resource": resource, "payload": payload})

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            dest = watch / "renamed.txt"
            src.rename(dest)
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.file.moved"
                    for event in emitted
                )
            )
        finally:
            await _stop_sensor(sensor, task)

    moved = next(
        event for event in emitted if event["event"] == "sensor.filesystem.file.moved"
    )
    assert moved["payload"]["source_path"] == str(src)
    assert moved["payload"]["destination_path"] == str(dest)


@pytest.mark.asyncio
async def test_filesystem_directory_events_are_opt_in(tmp_path: Path) -> None:
    watch = tmp_path / "w"
    watch.mkdir()
    cfg = FileSystemSensor.Config(
        name="fs-dirs",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["created"],
        include_directories=True,
    )
    sensor = FileSystemSensor(cfg)
    emitted: list[dict[str, object]] = []

    async def capture(*, event, resource, payload, occurred=None, related=None):
        emitted.append({"event": event, "resource": resource, "payload": payload})

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            subdir = watch / "subdir"
            subdir.mkdir()
            await _wait_until(
                lambda: any(
                    event["event"] == "sensor.filesystem.directory.created"
                    for event in emitted
                )
            )
        finally:
            await _stop_sensor(sensor, task)

    directory_event = next(
        event
        for event in emitted
        if event["event"] == "sensor.filesystem.directory.created"
    )
    assert directory_event["payload"]["kind"] == "directory"
    assert directory_event["payload"]["is_directory"] is True
