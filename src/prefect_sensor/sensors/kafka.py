"""Kafka topic sensor (stub — use aiokafka in production)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Dict, List

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class KafkaTopicSensor(StatefulSensorMixin, BaseSensor):
    """Stub consumer that emits a sample message observation.

    When ``state_file`` is configured, the sensor persists the last-read
    offset per ``(topic, partition)`` as ``{topic: {partition: offset}}``
    and advances it on each emitted observation. A real aiokafka
    implementation would substitute committed-offset retrieval here; the
    same state file remains useful as a fallback / snapshot.
    """

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
        self._offsets: Dict[str, Dict[str, int]] = {}

    async def setup(self) -> None:
        loaded = self._load_state()
        self._offsets = loaded if isinstance(loaded, dict) else {}
        self.logger.info(
            "Connected to %s, topics=%s, resumed offsets=%r",
            self.config.bootstrap_servers,
            self.config.topics,
            self._offsets,
        )

    async def teardown(self) -> None:
        self._save_state(self._offsets)
        self.logger.info("Kafka consumer disconnected.")

    async def observe(self) -> AsyncIterator[SensorObservation]:
        await asyncio.sleep(0)
        topic = self.config.topics[0]
        partition = "0"
        next_offset = self._offsets.get(topic, {}).get(partition, 42)

        yield SensorObservation(
            event_type="message.received",
            resource_id=f"kafka.topic.{topic}",
            payload={
                "topic": topic,
                "partition": int(partition),
                "offset": next_offset,
                "key": "order-123",
                "value": '{"status": "shipped"}',
            },
        )

        self._offsets.setdefault(topic, {})[partition] = next_offset + 1
        self._save_state(self._offsets)
