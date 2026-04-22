"""Tests for FileSystemSensor."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from prefect_sensor.sensors.filesystem import FileSystemSensor


async def _one_cycle(sensor: FileSystemSensor) -> list:
    out = []
    async for obs in sensor.observe():
        out.append(obs)
    return out


@pytest.mark.asyncio
async def test_filesystem_created_modified_deleted(tmp_path: Path) -> None:
    watch = tmp_path / "w"
    watch.mkdir()
    cfg = FileSystemSensor.Config(
        name="fs-test",
        watch_paths=[str(watch)],
        patterns=["*.txt"],
        recursive=True,
        poll_interval_seconds=0.0,
    )
    sensor = FileSystemSensor(cfg)

    assert await _one_cycle(sensor) == []

    f = watch / "a.txt"
    f.write_text("hello", encoding="utf-8")
    evs = await _one_cycle(sensor)
    assert len(evs) == 1
    assert evs[0].event_type == "file.created"
    assert evs[0].payload["path"] == str(f)

    await asyncio.sleep(0.05)
    f.write_text("world", encoding="utf-8")
    evs = await _one_cycle(sensor)
    assert len(evs) == 1
    assert evs[0].event_type == "file.modified"

    f.unlink()
    evs = await _one_cycle(sensor)
    assert len(evs) == 1
    assert evs[0].event_type == "file.deleted"
    assert evs[0].payload["path"] == str(f)
