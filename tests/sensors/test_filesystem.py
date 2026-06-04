"""Tests for FileSystemSensor."""

from __future__ import annotations

import asyncio
import json
import os
import time
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


@pytest.mark.asyncio
async def test_filesystem_catchup_scan_emits_for_files_after_hwm(
    tmp_path: Path,
) -> None:
    watch = tmp_path / "w"
    watch.mkdir()

    old_file = watch / "old.txt"
    old_file.write_text("old", encoding="utf-8")
    old_mtime = old_file.stat().st_mtime

    time.sleep(0.05)
    fresh = watch / "fresh.txt"
    fresh.write_text("fresh", encoding="utf-8")
    # Make sure mtime is comfortably greater than HWM.
    new_mtime = old_mtime + 1.0
    os.utime(fresh, (new_mtime, new_mtime))

    state_file = tmp_path / "fs-state.json"
    state_file.write_text(json.dumps({"state": old_mtime}))

    cfg = FileSystemSensor.Config(
        name="fs-catchup",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["created"],
        state_file=str(state_file),
    )
    sensor = FileSystemSensor(cfg)
    emitted: list[dict[str, object]] = []

    async def capture(*, event, resource, payload, occurred=None, related=None):
        emitted.append({"event": event, "resource": resource, "payload": payload})

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            await _wait_until(
                lambda: any(
                    e["payload"].get("catchup") is True
                    and e["payload"]["path"].endswith("fresh.txt")
                    for e in emitted
                )
            )
        finally:
            await _stop_sensor(sensor, task)

    catchup = [e for e in emitted if e["payload"].get("catchup") is True]
    assert len(catchup) == 1
    assert catchup[0]["event"] == "sensor.filesystem.file.created"
    assert catchup[0]["payload"]["path"].endswith("fresh.txt")


@pytest.mark.asyncio
async def test_filesystem_first_run_writes_state(tmp_path: Path) -> None:
    """First run with no prior state_file writes the HWM to disk so subsequent
    runs can decide what to catch up. We don't assert on emitted events here
    because watchdog/FSEvents behaviour for pre-existing files is platform
    dependent — the catch-up semantics are exercised by the test above.
    """
    watch = tmp_path / "w"
    watch.mkdir()
    state_file = tmp_path / "fs-state.json"

    cfg = FileSystemSensor.Config(
        name="fs-firstrun",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        events=["created"],
        state_file=str(state_file),
    )
    sensor = FileSystemSensor(cfg)

    async def capture(*, event, resource, payload, occurred=None, related=None):
        pass

    with patch("prefect_sensor.base.emit_event_async", side_effect=capture):
        task = await _start_sensor(sensor)
        try:
            await asyncio.sleep(0.1)
        finally:
            await _stop_sensor(sensor, task)

    assert state_file.exists()
    persisted = json.loads(state_file.read_text())
    assert isinstance(persisted["state"], (int, float))
    assert persisted["state"] > 0
