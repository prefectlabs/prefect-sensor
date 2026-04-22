"""Kafka topic sensor (stub — use aiokafka in production)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import List

from prefect_sensor.base import BaseSensor
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class KafkaTopicSensor(BaseSensor):
    """Stub consumer that emits a sample message observation."""

    class Config(SensorConfig):
        bootstrap_servers: str = "localhost:9092"
        topics: List[str]
        group_id: str = "prefect-sensor"
        auto_offset_reset: str = "latest"
        poll_timeout_ms: int = 1000
        emit_prefix: str = "sensor.kafka"

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._consumer = None

    async def setup(self) -> None:
        self.logger.info(
            "Connected to %s, topics=%s",
            self.config.bootstrap_servers,
            self.config.topics,
        )

    async def teardown(self) -> None:
        self.logger.info("Kafka consumer disconnected.")

    async def observe(self) -> AsyncIterator[SensorObservation]:
        await asyncio.sleep(0)
        yield SensorObservation(
            event_type="message.received",
            resource_id=f"kafka.topic.{self.config.topics[0]}",
            payload={
                "topic": self.config.topics[0],
                "partition": 0,
                "offset": 42,
                "key": "order-123",
                "value": '{"status": "shipped"}',
            },
        )
