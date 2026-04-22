"""Importable sensors for YAML-based tests (``tests.helpers.*``)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from prefect_sensor.base import BaseSensor
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class DummySensor(BaseSensor):
    """Emits ``n`` observations then stops the sensor."""

    class Config(SensorConfig):
        n: int = 3

    config: Config

    async def observe(self) -> AsyncIterator[SensorObservation]:
        for i in range(self.config.n):
            yield SensorObservation(
                event_type="test.tick",
                resource_id=f"dummy:{self.config.name}:{i}",
                payload={"i": i},
            )
        self.request_stop()


class FlakySensor(BaseSensor):
    """Always raises from ``observe``."""

    class Config(SensorConfig):
        pass

    config: Config

    async def observe(self) -> AsyncIterator[SensorObservation]:
        msg = "flaky"
        raise RuntimeError(msg)
        yield  # pragma: no cover
