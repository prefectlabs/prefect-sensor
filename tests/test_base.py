"""Tests for BaseSensor and StatefulSensorMixin."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation, SensorState
from prefect_sensor._internal.schema.config import SensorConfig
from tests.helpers import DummySensor, FlakySensor


@pytest.mark.asyncio
async def test_dummy_sensor_emits_and_stops() -> None:
    cfg = DummySensor.Config(name="d", n=2)
    sensor = DummySensor(cfg)

    with patch("prefect_sensor.base.emit_event_async", new_callable=AsyncMock) as emit:
        await sensor.run()

    assert emit.await_count == 2
    assert sensor._events_emitted == 2
    assert sensor.state == SensorState.IDLE
    calls = emit.await_args_list
    assert calls[0].kwargs["event"] == "sensor.test.tick"
    assert calls[0].kwargs["resource"]["prefect.resource.id"] == "dummy:d:0"
    assert calls[0].kwargs["related"][0]["prefect.resource.role"] == "sensor"


@pytest.mark.asyncio
async def test_flaky_sensor_halts_after_max_errors() -> None:
    cfg = FlakySensor.Config(
        name="f",
        error_backoff_seconds=0.0,
        max_consecutive_errors=2,
    )
    sensor = FlakySensor(cfg)

    with patch("prefect_sensor.base.emit_event_async", new_callable=AsyncMock):
        await sensor.run()

    # Run loop sets ERRORED then ``finally`` resets to IDLE after teardown.
    assert sensor.state == SensorState.IDLE
    assert sensor._errors >= 2
    hb = sensor.heartbeat()
    assert hb.errors >= 2
    assert hb.last_error is not None


@pytest.mark.asyncio
async def test_request_stop_mid_run() -> None:
    class SlowDummy(DummySensor):
        async def observe(self):
            yield SensorObservation(
                event_type="one",
                resource_id="r1",
                payload={},
            )
            self.request_stop()
            yield SensorObservation(
                event_type="two",
                resource_id="r2",
                payload={},
            )

    cfg = SlowDummy.Config(name="s", n=99)
    sensor = SlowDummy(cfg)

    with patch("prefect_sensor.base.emit_event_async", new_callable=AsyncMock) as emit:
        await sensor.run()

    assert emit.await_count == 1


def test_stateful_mixin_fingerprint() -> None:
    class C(StatefulSensorMixin, BaseSensor):
        class Config(SensorConfig):
            pass

        config: Config

        async def observe(self):
            if False:
                yield SensorObservation(  # pragma: no cover
                    event_type="x",
                    resource_id="y",
                )

    c = C(C.Config(name="mixin"))
    assert c._fingerprint({"a": 1}) == c._fingerprint({"a": 1})
    assert c._fingerprint({"a": 1}) != c._fingerprint({"a": 2})
    assert c._is_new({"x": 1}) is True
    assert c._is_new({"x": 1}) is False
