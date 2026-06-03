"""Tests for the Kafka sensor."""

from __future__ import annotations

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
    agen = s.observe().__aiter__()
    obs = await agen.__anext__()
    assert obs.event_type == "message.received"
    assert obs.resource_id == "kafka.topic.orders"
    assert obs.payload["topic"] == "orders"
    assert obs.payload["offset"] == 42
