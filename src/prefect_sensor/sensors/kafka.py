"""Kafka topic sensor backed by aiokafka."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from typing import Any, Literal

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import Field, field_validator

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor._internal.schema import SensorObservation
from prefect_sensor._internal.schema.config import SensorConfig


class KafkaTopicSensor(StatefulSensorMixin, BaseSensor):
    """Consume Kafka records and emit one Prefect event per record.

    Kafka offsets are committed only after the observation's Prefect event is
    emitted successfully. This provides at-least-once delivery: an event can be
    duplicated when emission succeeds but committing its offset fails, but a
    failed emission does not advance the consumer group.

    When ``state_file`` is configured, the sensor snapshots the next offset per
    ``(topic, partition)`` as ``{topic: {partition: offset}}``. Broker commits
    are authoritative; the local snapshot is used only when a newly assigned
    partition has no committed group offset.
    """

    class Config(SensorConfig):
        bootstrap_servers: str = "localhost:9092"
        topics: list[str] = Field(min_length=1)
        group_id: str = Field(default="prefect-sensor", min_length=1)
        auto_offset_reset: Literal["earliest", "latest"] = "latest"
        poll_timeout_ms: int = Field(default=1000, ge=0)
        emit_prefix: str = "sensor.kafka"

        @field_validator("topics")
        @classmethod
        def _validate_topics(cls, topics: list[str]) -> list[str]:
            if any(not topic.strip() for topic in topics):
                raise ValueError("Kafka topics must not be empty")
            return topics

        @field_validator("group_id")
        @classmethod
        def _validate_group_id(cls, group_id: str) -> str:
            if not group_id.strip():
                raise ValueError("Kafka group_id must not be empty")
            return group_id

    config: Config

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._consumer: AIOKafkaConsumer | None = None
        self._pending: deque[tuple[TopicPartition, ConsumerRecord]] = deque()
        self._offsets: dict[str, dict[str, int]] = {}
        self._rebalance_listener: _KafkaRebalanceListener | None = None

    async def setup(self) -> None:
        self._offsets = self._normalize_offsets(self._load_state())
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=False,
        )
        listener = _KafkaRebalanceListener(self)
        consumer.subscribe(topics=self.config.topics, listener=listener)
        self._consumer = consumer
        self._rebalance_listener = listener
        try:
            await consumer.start()
        except Exception:
            self._consumer = None
            self._rebalance_listener = None
            await consumer.stop()
            raise
        self.logger.info(
            "Connected to %s, topics=%s, resumed offsets=%r",
            self.config.bootstrap_servers,
            self.config.topics,
            self._offsets,
        )

    async def teardown(self) -> None:
        consumer = self._consumer
        self._consumer = None
        self._rebalance_listener = None
        try:
            self._save_state(self._offsets)
        finally:
            if consumer is not None:
                await consumer.stop()
            self._pending.clear()
        self.logger.info("Kafka consumer disconnected")

    async def observe(self) -> AsyncIterator[SensorObservation]:
        consumer = self._consumer
        if consumer is None:
            raise RuntimeError(
                "KafkaTopicSensor.setup() must be called before observe()."
            )

        if not self._pending:
            batches = await consumer.getmany(timeout_ms=self.config.poll_timeout_ms)
            for topic_partition, records in batches.items():
                for record in records:
                    self._pending.append((topic_partition, record))

        if not self._pending:
            return

        _topic_partition, record = self._pending[0]
        yield SensorObservation(
            event_type="message.received",
            resource_id=f"kafka.topic.{record.topic}",
            payload={
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "key": self._decode(record.key),
                "value": self._decode(record.value),
            },
        )

    async def acknowledge(self, observation: SensorObservation) -> None:
        """Commit and snapshot the pending record after Prefect emission."""
        consumer = self._consumer
        if consumer is None:
            raise RuntimeError("Kafka consumer is not running")
        if not self._pending:
            raise RuntimeError("No pending Kafka record to acknowledge")

        topic_partition, record = self._pending[0]
        next_offset = record.offset + 1
        await consumer.commit({topic_partition: next_offset})
        self._offsets.setdefault(record.topic, {})[str(record.partition)] = next_offset
        self._save_state(self._offsets)
        self._pending.popleft()

    async def _restore_offsets(self, assigned: set[TopicPartition]) -> None:
        consumer = self._consumer
        if consumer is None:
            return

        for topic_partition in assigned:
            saved_offset = self._offsets.get(topic_partition.topic, {}).get(
                str(topic_partition.partition)
            )
            if saved_offset is None:
                continue
            committed_offset = await consumer.committed(topic_partition)
            if committed_offset is None:
                consumer.seek(topic_partition, saved_offset)
                self.logger.info(
                    "Restored local Kafka offset %d for %s[%d]",
                    saved_offset,
                    topic_partition.topic,
                    topic_partition.partition,
                )

    def _drop_pending(self, revoked: set[TopicPartition]) -> None:
        if not revoked or not self._pending:
            return
        self._pending = deque(
            pending for pending in self._pending if pending[0] not in revoked
        )

    def _normalize_offsets(self, loaded: Any) -> dict[str, dict[str, int]]:
        if not isinstance(loaded, dict):
            return {}

        offsets: dict[str, dict[str, int]] = {}
        for topic, partitions in loaded.items():
            if not isinstance(topic, str) or not isinstance(partitions, dict):
                continue
            normalized_partitions: dict[str, int] = {}
            for partition, offset in partitions.items():
                if (
                    isinstance(partition, str)
                    and isinstance(offset, int)
                    and not isinstance(offset, bool)
                    and offset >= 0
                ):
                    normalized_partitions[partition] = offset
            if normalized_partitions:
                offsets[topic] = normalized_partitions
        return offsets

    @staticmethod
    def _decode(value: bytes | str | None) -> str | None:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value


class _KafkaRebalanceListener(ConsumerRebalanceListener):
    """Keep local pending/state offsets aligned with group assignments."""

    def __init__(self, sensor: KafkaTopicSensor) -> None:
        self._sensor = sensor

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        self._sensor._drop_pending(revoked)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        await self._sensor._restore_offsets(assigned)
