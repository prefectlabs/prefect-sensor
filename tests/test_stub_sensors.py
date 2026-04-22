"""Tests for Kafka / SFTP / SQL sensors."""

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
async def test_sftp_sensor_e2e(sftp_server) -> None:
    remote_dir = sftp_server.root / "incoming" / "reports"
    remote_dir.mkdir(parents=True)
    report = remote_dir / "report.csv"
    report.write_text("version 1\n")

    s = SFTPSensor(
        SFTPSensor.Config(
            name="s",
            hostname=sftp_server.host,
            port=sftp_server.port,
            username=sftp_server.username,
            password=sftp_server.password,
            remote_directories=["/incoming/reports"],
            poll_interval_seconds=0,
        )
    )
    await s.setup()
    try:
        agen = s.observe().__aiter__()
        obs = await agen.__anext__()
        assert obs.event_type == "file.appeared"
        assert (
            obs.resource_id == f"sftp:{sftp_server.host}:/incoming/reports/report.csv"
        )
        assert obs.payload["remote_path"] == "/incoming/reports/report.csv"
        assert obs.payload["size"] == len("version 1\n")

        report.write_text("version 2 with more bytes\n")
        report.touch()

        agen = s.observe().__aiter__()
        obs = await agen.__anext__()
        assert obs.event_type == "file.changed"
        assert obs.payload["remote_path"] == "/incoming/reports/report.csv"
    finally:
        await s.teardown()


@pytest.mark.asyncio
async def test_sql_sensor_stub() -> None:
    s = SQLSensor(SQLSensor.Config(name="q", tracking_column="id"))
    agen = s.observe().__aiter__()
    obs = await agen.__anext__()
    assert obs.event_type == "row.detected"
    assert obs.resource_id == "sql:id:1001"
    assert obs.payload["customer"] == "acme"
