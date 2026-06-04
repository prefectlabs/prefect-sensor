---
title: Kafka Sensor
---

# Kafka Sensor

`prefect_sensor.sensors.kafka.KafkaTopicSensor` is intended to consume one or more Kafka topics and emit a `message.received` event for every record it sees.

::: warning Stub implementation
The current `KafkaTopicSensor` does not yet connect to a Kafka broker. On each `observe()` tick it yields a single hardcoded `message.received` observation for documentation and demo purposes. A production-grade consumer (e.g. via [`aiokafka`](https://aiokafka.readthedocs.io/)) is planned. The config fields below are the documented surface and will be honoured by the real implementation when it lands.
:::

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bootstrap_servers` | `str` | `"localhost:9092"` | Comma-separated list of Kafka bootstrap servers. |
| `topics` | `list[str]` | *required* | Topics to subscribe to. |
| `group_id` | `str` | `"prefect-sensor"` | Kafka consumer group ID. |
| `auto_offset_reset` | `str` | `"latest"` | Offset reset policy (`latest` or `earliest`) for new consumer groups. |
| `poll_timeout_ms` | `int` | `1000` | Per-poll timeout in milliseconds passed to the underlying consumer. |
| `state_file` | `str \| null` | `null` | Path to a JSON file used to persist last-read offsets as `{topic: {partition: offset}}`. When set, offsets are loaded on `setup()` and saved after every emitted observation and on teardown so restarts resume without replay. |
| `emit_prefix` | `str` | `"sensor.kafka"` | Prefix prepended to every emitted event type. |

## Example

```yaml
sensors:
  - prefect_sensor.sensors.kafka.KafkaTopicSensor:
      name: orders consumer
      bootstrap_servers: broker.example.com:9092
      topics:
        - orders
      group_id: prefect-sensor-orders
```

See [Getting Started](../getting-started.md) for how to run a config.

## Events emitted

With the default `emit_prefix`, the sensor emits:

- `sensor.kafka.message.received` — one event per record consumed.

Each observation carries:

- **Resource ID** — `kafka.topic.{topic_name}`
- **Payload** — `topic`, `partition`, `offset`, `key`, `value`. Values are passed through as strings; once the real consumer is wired up, deserialization (JSON, Avro, etc.) will be the caller's responsibility.

## State persistence

When `state_file` is set, the sensor persists a JSON document of the form `{"state": {"<topic>": {"<partition>": <offset>}}}` and atomically replaces it (temp file + `os.replace`) after each emitted observation and on teardown. On startup the document is read back to seed the in-memory offset map, so a restarted sensor resumes from the last-acknowledged offset rather than the configured `auto_offset_reset` floor. A real `aiokafka` implementation will substitute committed-offset retrieval from the broker for primary recovery; the state file remains useful as a local fallback / snapshot.
