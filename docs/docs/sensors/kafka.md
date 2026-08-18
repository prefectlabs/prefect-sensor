---
title: Kafka Sensor
---

# Kafka Sensor

`prefect_sensor.sensors.kafka.KafkaTopicSensor` consumes one or more Kafka
topics with `aiokafka` and emits a `message.received` event for every record.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bootstrap_servers` | `str` | `"localhost:9092"` | Comma-separated Kafka bootstrap servers. This release supports plaintext connections. |
| `topics` | `list[str]` | *required* | One or more nonempty topic names to subscribe to. |
| `group_id` | `str` | `"prefect-sensor"` | Nonempty Kafka consumer group ID used for broker commits. |
| `auto_offset_reset` | `"earliest" \| "latest"` | `"latest"` | Starting position when neither a broker commit nor local snapshot exists. |
| `poll_timeout_ms` | `int` | `1000` | Nonnegative timeout passed to each Kafka batch poll. |
| `state_file` | `str \| null` | `null` | Optional local snapshot containing the next offset for each topic partition. |
| `emit_prefix` | `str` | `"sensor.kafka"` | Prefix prepended to every emitted event type. |

```yaml
sensors:
  - prefect_sensor.sensors.kafka.KafkaTopicSensor:
      name: orders consumer
      bootstrap_servers: broker.example.com:9092
      topics:
        - orders
      group_id: prefect-sensor-orders
      auto_offset_reset: earliest
      state_file: /var/lib/prefect-sensor/orders.json
```

See [Getting Started](../getting-started.md) for how to run a sensor config.

## Events emitted

With the default `emit_prefix`, every consumed record becomes
`sensor.kafka.message.received`.

- **Resource ID** — `kafka.topic.{topic_name}`
- **Payload** — `topic`, `partition`, `offset`, `key`, and `value`

Byte keys and values are decoded as UTF-8 with invalid bytes replaced. Kafka
nulls remain `null`, and strings supplied by a custom deserializer pass through
unchanged.

## Delivery and offsets

The sensor disables Kafka auto-commit. It retains each fetched record until the
corresponding Prefect event is emitted, then commits `record.offset + 1` and
updates the local state snapshot. A failed Prefect emission therefore leaves
the record pending for retry.

This is **at-least-once delivery**. If Prefect accepts an event but the following
Kafka commit fails, the record is emitted again on retry and Prefect may receive
a duplicate. Downstream automations that require deduplication can use the
`topic`, `partition`, and `offset` payload fields as a stable record identity.

Kafka consumer-group commits are authoritative on restart. When an assigned
partition has no broker commit, the sensor seeks to the next offset stored in
`state_file`; when neither exists, `auto_offset_reset` controls the starting
position. The file uses this shape:

```json
{"state": {"orders": {"0": 43, "1": 18}}}
```

Writes use an atomic temporary-file replacement. Records from partitions lost
during a group rebalance are removed from the local pending queue so the new
partition owner can replay them from the broker commit.

## Runnable Kafka and cgen example

The repository includes a complete
[Docker Compose example](https://github.com/prefectlabs/prefect-sensor/tree/main/examples/kafka)
with Redpanda, Redpanda Console, the published `cgen` Kafka producer, and a
locally built sensor container. It targets Prefect Cloud and requires
`PREFECT_API_URL` and `PREFECT_API_KEY`.
