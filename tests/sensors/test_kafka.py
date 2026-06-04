"""Tests for the Kafka sensor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from prefect_sensor.sensors.kafka import KafkaTopicSensor


@pytest.mark.asyncio
async def test_kafka_topic_sensor_stub() -> None:
    s = KafkaTopicSensor(
        KafkaTopicSensor.Config(
            name="k",
            topics=["orders"],
            bootstrap_servers="localhost:9092",
        )
    )
    await s.setup()
    try:
        agen = s.observe().__aiter__()
        obs = await agen.__anext__()
        assert obs.event_type == "message.received"
        assert obs.resource_id == "kafka.topic.orders"
        assert obs.payload["topic"] == "orders"
        assert obs.payload["offset"] == 42
    finally:
        await s.teardown()


@pytest.mark.asyncio
async def test_kafka_topic_sensor_persists_offsets(tmp_path: Path) -> None:
    state_file = str(tmp_path / "kafka.json")
    cfg_kwargs = dict(
        name="k",
        topics=["orders"],
        bootstrap_servers="localhost:9092",
        state_file=state_file,
    )

    s1 = KafkaTopicSensor(KafkaTopicSensor.Config(**cfg_kwargs))
    await s1.setup()
    try:
        async for _ in s1.observe():
            pass
    finally:
        await s1.teardown()

    persisted = json.loads(Path(state_file).read_text())
    assert persisted["state"] == {"orders": {"0": 43}}

    s2 = KafkaTopicSensor(KafkaTopicSensor.Config(**cfg_kwargs))
    await s2.setup()
    try:
        async for obs in s2.observe():
            assert obs.payload["offset"] == 43
    finally:
        await s2.teardown()

    persisted = json.loads(Path(state_file).read_text())
    assert persisted["state"] == {"orders": {"0": 44}}
