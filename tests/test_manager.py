"""Tests for SensorManager."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from prefect_sensor.base import BaseSensor
from prefect_sensor._internal.schema import SensorObservation, SensorState
from prefect_sensor.manager import SensorManager
from tests.helpers import DummySensor


@pytest.mark.asyncio
async def test_from_sensors_runs_concurrently() -> None:
    a = DummySensor(DummySensor.Config(name="a", n=1))
    b = DummySensor(DummySensor.Config(name="b", n=1))
    manager = SensorManager.from_sensors(a, b)

    with patch("prefect_sensor.base.emit_event_async", new_callable=AsyncMock) as emit:
        await manager.start()

    assert emit.await_count == 2


@pytest.mark.asyncio
async def test_stop_cancels_tasks() -> None:
    from prefect_sensor.sensors.filesystem import FileSystemSensor

    watch = Path(__file__).parent / "fixtures"
    s = FileSystemSensor(
        FileSystemSensor.Config(
            name="slow-fs",
            watch_paths=[str(watch)],
            patterns=["*.yaml"],
            events=["created"],
        )
    )
    manager = SensorManager.from_sensors(s)

    with patch("prefect_sensor.base.emit_event_async", new_callable=AsyncMock):
        task = asyncio.create_task(manager.start())
        await asyncio.sleep(0.05)
        await manager.stop()
        await task

    assert s.state.value == "idle"


@pytest.mark.asyncio
async def test_from_yaml_roundtrip(sample_sensor_yaml_path: str) -> None:
    manager = SensorManager.from_yaml(sample_sensor_yaml_path)
    assert len(manager.sensors) == 2
    names = {s.config.name for s in manager.sensors}
    assert names == {"alpha", "beta"}


@pytest.mark.asyncio
async def test_start_logs_summary_periodically(caplog) -> None:
    class WaitingSensor(BaseSensor):
        class Config(DummySensor.Config):
            pass

        config: Config

        async def observe(self):
            if False:
                yield SensorObservation(  # pragma: no cover
                    event_type="tick",
                    resource_id="waiting:noop",
                )
            while not self._stop_event.is_set():
                await asyncio.sleep(0.01)

    sensor = WaitingSensor(WaitingSensor.Config(name="waiting"))
    manager = SensorManager.from_sensors(sensor)

    caplog.set_level(logging.INFO, logger="prefect.sensors")
    task = asyncio.create_task(manager.start(summary_interval_seconds=0.01))
    await asyncio.sleep(0.04)
    await manager.stop()
    await task

    assert "SensorManager: 1 sensor(s) configured" in caplog.text
    assert manager._summary_task is None
    assert sensor.state == SensorState.IDLE


@pytest.mark.asyncio
async def test_start_rejects_non_positive_summary_interval() -> None:
    manager = SensorManager.from_sensors(
        DummySensor(DummySensor.Config(name="alpha", n=1))
    )

    with pytest.raises(ValueError, match="greater than 0"):
        await manager.start(summary_interval_seconds=0)


def test_summary(sample_sensor_yaml_path: str) -> None:
    manager = SensorManager.from_yaml(sample_sensor_yaml_path)
    text = manager.summary()
    assert "SensorManager: 2 sensor(s)" in text
    assert "alpha" in text
