"""Tests for the Kafka sensor."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from aiokafka.structs import TopicPartition

from prefect_sensor.sensors.kafka import KafkaTopicSensor


class FakeConsumer:
    """Small aiokafka consumer stand-in used by the isolated unit tests."""

    def __init__(self, *, event_log: list[str] | None = None) -> None:
        self.event_log = event_log
        self.batches: list[dict[TopicPartition, list[SimpleNamespace]]] = []
        self.committed_offsets: dict[TopicPartition, int | None] = {}
        self.commit_calls: list[dict[TopicPartition, int]] = []
        self.committed_calls: list[TopicPartition] = []
        self.seek_calls: list[tuple[TopicPartition, int]] = []
        self.getmany_timeouts: list[int] = []
        self.start_calls = 0
        self.stop_calls = 0
        self.start_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.topics: list[str] | None = None
        self.listener = None

    def subscribe(self, *, topics, listener) -> None:
        self.topics = list(topics)
        self.listener = listener

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    async def stop(self) -> None:
        self.stop_calls += 1

    async def getmany(self, *, timeout_ms: int):
        self.getmany_timeouts.append(timeout_ms)
        if self.batches:
            return self.batches.pop(0)
        return {}

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        if self.event_log is not None:
            self.event_log.append("commit")
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_calls.append(offsets)

    async def committed(self, topic_partition: TopicPartition) -> int | None:
        self.committed_calls.append(topic_partition)
        return self.committed_offsets.get(topic_partition)

    def seek(self, topic_partition: TopicPartition, offset: int) -> None:
        self.seek_calls.append((topic_partition, offset))


def _record(
    topic: str,
    partition: int,
    offset: int,
    *,
    key: bytes | str | None = None,
    value: bytes | str | None = b"payload",
) -> SimpleNamespace:
    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
    )


def _sensor(**kwargs) -> KafkaTopicSensor:
    config = {"name": "kafka-test", "topics": ["orders"], **kwargs}
    return KafkaTopicSensor(KafkaTopicSensor.Config(**config))


async def _collect(sensor: KafkaTopicSensor) -> list:
    return [observation async for observation in sensor.observe()]


@pytest.mark.asyncio
async def test_setup_configures_subscribes_and_stops_consumer() -> None:
    fake = FakeConsumer()
    sensor = _sensor(
        bootstrap_servers="broker:19092",
        group_id="orders-group",
        auto_offset_reset="earliest",
    )

    with patch(
        "prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake
    ) as consumer_cls:
        await sensor.setup()
        consumer_cls.assert_called_once_with(
            bootstrap_servers="broker:19092",
            group_id="orders-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        assert fake.topics == ["orders"]
        assert fake.listener is not None
        assert fake.start_calls == 1

        await sensor.teardown()
        await sensor.teardown()

    assert fake.stop_calls == 1


@pytest.mark.asyncio
async def test_setup_stops_consumer_when_start_fails() -> None:
    fake = FakeConsumer()
    fake.start_error = RuntimeError("broker unavailable")
    sensor = _sensor()

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        with pytest.raises(RuntimeError, match="broker unavailable"):
            await sensor.setup()

    assert fake.stop_calls == 1
    assert sensor._consumer is None


@pytest.mark.asyncio
async def test_observe_requires_setup() -> None:
    sensor = _sensor()
    with pytest.raises(RuntimeError, match=r"setup\(\)"):
        await _collect(sensor)


@pytest.mark.asyncio
async def test_empty_poll_uses_configured_timeout() -> None:
    fake = FakeConsumer()
    sensor = _sensor(poll_timeout_ms=321)

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            assert await _collect(sensor) == []
        finally:
            await sensor.teardown()

    assert fake.getmany_timeouts == [321]


@pytest.mark.asyncio
async def test_observe_queues_records_and_decodes_payloads() -> None:
    fake = FakeConsumer()
    topic_partition = TopicPartition("orders", 2)
    fake.batches.append(
        {
            topic_partition: [
                _record(
                    "orders",
                    2,
                    8,
                    key=b"order-123",
                    value=b'{"status":"shipped"}',
                ),
                _record("orders", 2, 9, key=None, value=b"bad-utf8:\xff"),
            ]
        }
    )
    sensor = _sensor()

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            first = (await _collect(sensor))[0]
            retry = (await _collect(sensor))[0]
            assert retry.payload == first.payload
            assert fake.getmany_timeouts == [1000]

            assert first.event_type == "message.received"
            assert first.resource_id == "kafka.topic.orders"
            assert first.payload == {
                "topic": "orders",
                "partition": 2,
                "offset": 8,
                "key": "order-123",
                "value": '{"status":"shipped"}',
            }

            await sensor.acknowledge(first)
            second = (await _collect(sensor))[0]
            assert second.payload["offset"] == 9
            assert second.payload["key"] is None
            assert second.payload["value"] == "bad-utf8:\ufffd"
        finally:
            await sensor.teardown()


@pytest.mark.asyncio
async def test_acknowledge_commits_next_offset_and_persists_state(
    tmp_path: Path,
) -> None:
    fake = FakeConsumer()
    topic_partition = TopicPartition("orders", 0)
    fake.batches.append(
        {topic_partition: [_record("orders", 0, 42, value="already-decoded")]}
    )
    state_file = tmp_path / "kafka.json"
    sensor = _sensor(state_file=str(state_file))

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            observation = (await _collect(sensor))[0]
            await sensor.acknowledge(observation)
        finally:
            await sensor.teardown()

    assert fake.commit_calls == [{topic_partition: 43}]
    assert json.loads(state_file.read_text()) == {"state": {"orders": {"0": 43}}}


@pytest.mark.asyncio
async def test_commit_failure_keeps_record_pending_without_advancing_state(
    tmp_path: Path,
) -> None:
    fake = FakeConsumer()
    topic_partition = TopicPartition("orders", 0)
    fake.batches.append({topic_partition: [_record("orders", 0, 7)]})
    fake.commit_error = RuntimeError("commit failed")
    state_file = tmp_path / "kafka.json"
    sensor = _sensor(state_file=str(state_file))

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            observation = (await _collect(sensor))[0]
            with pytest.raises(RuntimeError, match="commit failed"):
                await sensor.acknowledge(observation)

            retry = (await _collect(sensor))[0]
            assert retry.payload["offset"] == 7
            assert sensor._offsets == {}

            fake.commit_error = None
            await sensor.acknowledge(retry)
        finally:
            await sensor.teardown()

    assert fake.commit_calls == [{topic_partition: 8}]
    assert json.loads(state_file.read_text()) == {"state": {"orders": {"0": 8}}}


@pytest.mark.asyncio
async def test_local_state_only_restores_partitions_without_broker_commit(
    tmp_path: Path,
) -> None:
    first = TopicPartition("orders", 0)
    second = TopicPartition("orders", 1)
    state_file = tmp_path / "kafka.json"
    state_file.write_text(json.dumps({"state": {"orders": {"0": 10, "1": 20}}}))
    fake = FakeConsumer()
    fake.committed_offsets = {first: None, second: 99}
    sensor = _sensor(state_file=str(state_file))

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            await fake.listener.on_partitions_assigned({first, second})
        finally:
            await sensor.teardown()

    assert set(fake.committed_calls) == {first, second}
    assert fake.seek_calls == [(first, 10)]


@pytest.mark.asyncio
async def test_revocation_discards_pending_records_for_revoked_partition() -> None:
    first = TopicPartition("orders", 0)
    second = TopicPartition("orders", 1)
    fake = FakeConsumer()
    fake.batches.append(
        {
            first: [_record("orders", 0, 1)],
            second: [_record("orders", 1, 5)],
        }
    )
    sensor = _sensor()

    with patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake):
        await sensor.setup()
        try:
            await _collect(sensor)
            await fake.listener.on_partitions_revoked({first})
            assert [topic_partition for topic_partition, _ in sensor._pending] == [
                second
            ]
        finally:
            await sensor.teardown()


@pytest.mark.asyncio
async def test_run_emits_before_committing() -> None:
    order: list[str] = []
    fake = FakeConsumer(event_log=order)
    topic_partition = TopicPartition("orders", 0)
    fake.batches.append({topic_partition: [_record("orders", 0, 3)]})
    sensor = _sensor()

    async def emit(*args, **kwargs) -> None:
        order.append("emit")
        sensor.request_stop()

    with (
        patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake),
        patch("prefect_sensor.base.emit_event_async", side_effect=emit),
    ):
        await sensor.run()

    assert order == ["emit", "commit"]


@pytest.mark.asyncio
async def test_failed_prefect_emit_does_not_commit_or_advance_state(
    tmp_path: Path,
) -> None:
    fake = FakeConsumer()
    topic_partition = TopicPartition("orders", 0)
    fake.batches.append({topic_partition: [_record("orders", 0, 3)]})
    state_file = tmp_path / "kafka.json"
    sensor = _sensor(
        state_file=str(state_file),
        max_consecutive_errors=1,
        error_backoff_seconds=0,
    )

    with (
        patch("prefect_sensor.sensors.kafka.AIOKafkaConsumer", return_value=fake),
        patch(
            "prefect_sensor.base.emit_event_async",
            side_effect=RuntimeError("Prefect unavailable"),
        ),
    ):
        await sensor.run()

    assert fake.commit_calls == []
    assert json.loads(state_file.read_text()) == {"state": {}}


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"topics": []}, "too_short"),
        ({"topics": [" "]}, "must not be empty"),
        ({"group_id": ""}, "too_short"),
        ({"group_id": " "}, "must not be empty"),
        ({"auto_offset_reset": "middle"}, "literal_error"),
        ({"poll_timeout_ms": -1}, "greater_than_equal"),
    ],
)
def test_config_validation(overrides: dict, message: str) -> None:
    values = {"name": "k", "topics": ["orders"], **overrides}
    with pytest.raises(ValueError, match=message):
        KafkaTopicSensor.Config(**values)
