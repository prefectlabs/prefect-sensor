"""Tests for BaseSensor and StatefulSensorMixin."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
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


class _StateSensor(StatefulSensorMixin, BaseSensor):
    class Config(SensorConfig):
        pass

    config: Config

    async def observe(self):
        if False:
            yield SensorObservation(  # pragma: no cover
                event_type="x",
                resource_id="y",
            )


class _DatetimeStateSensor(_StateSensor):
    def _deserialize_state(self, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value


def test_stateful_mixin_returns_none_without_state_file() -> None:
    sensor = _StateSensor(_StateSensor.Config(name="s"))
    assert sensor._load_state() is None
    sensor._save_state(42)


def test_stateful_mixin_returns_none_when_file_missing(tmp_path: Path) -> None:
    state_file = tmp_path / "missing.json"
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    assert sensor._load_state() is None


def test_stateful_mixin_roundtrips_int(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    sensor._save_state(123)
    assert json.loads(state_file.read_text()) == {"state": 123}
    assert sensor._load_state() == 123


def test_stateful_mixin_roundtrips_dict(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    offsets = {"orders": {"0": 42, "1": 7}}
    sensor._save_state(offsets)
    assert sensor._load_state() == offsets


def test_stateful_mixin_roundtrips_datetime_via_override(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    sensor = _DatetimeStateSensor(
        _DatetimeStateSensor.Config(name="s", state_file=str(state_file))
    )
    when = datetime(2024, 6, 1, 12, 30, tzinfo=timezone.utc)
    sensor._save_state(when)
    assert sensor._load_state() == when


def test_stateful_mixin_backward_compat_hwm_key(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"hwm": 99}))
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    assert sensor._load_state() == 99


def test_stateful_mixin_corrupt_file_returns_none(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("not json at all")
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    assert sensor._load_state() is None


def test_stateful_mixin_does_not_leave_tempfiles(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    sensor = _StateSensor(_StateSensor.Config(name="s", state_file=str(state_file)))
    sensor._save_state(7)
    assert sensor._load_state() == 7
    remaining = [p.name for p in tmp_path.iterdir() if p.name != "state.json"]
    assert remaining == []
