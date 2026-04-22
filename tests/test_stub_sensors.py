"""Tests for stub Kafka / SFTP / SQL sensors."""

from __future__ import annotations

import pytest

from prefect_sensor.sensors.kafka import KafkaTopicSensor
from prefect_sensor.sensors.sftp import SFTPSensor
from prefect_sensor.sensors.sql import SQLSensor


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


@pytest.mark.asyncio
async def test_sftp_sensor_stub() -> None:
    s = SFTPSensor(
        SFTPSensor.Config(
            name="s",
            hostname="sftp.example.com",
            remote_directories=["/incoming/reports"],
        )
    )
    agen = s.observe().__aiter__()
    obs = await agen.__anext__()
    assert obs.event_type == "file.appeared"
    assert "sftp.example.com" in obs.resource_id
    assert obs.payload["remote_path"] == "/incoming/reports/report.csv"


@pytest.mark.asyncio
async def test_sql_sensor_stub() -> None:
    s = SQLSensor(SQLSensor.Config(name="q", tracking_column="id"))
    agen = s.observe().__aiter__()
    obs = await agen.__anext__()
    assert obs.event_type == "row.detected"
    assert obs.resource_id == "sql:id:1001"
    assert obs.payload["customer"] == "acme"
