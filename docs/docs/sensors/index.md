# Sensors

Sensors are configured via YAML file. 

The configuration is structured with a top level `sensors` key. 

Each sensor is configured using the MRO import path as a key.

For example:

```yaml
sensors:
  - path.to.SensorClass:
      key: value
```

## Common configuration

Every sensor accepts the following fields in addition to its own configuration:

- **`name`** — Human-readable identifier used in logs and as part of the emitted event source.
- **`emit_prefix`** — Prefix prepended to every event type before emission. Each built-in sensor sets a sensible default (for example, `sensor.filesystem`), so a `file.created` observation is emitted as `sensor.filesystem.file.created`.

Environment variable interpolation works in any field. Use <code v-pre>{{ env('VAR') }}</code> for required values and <code v-pre>{{ env('VAR', 'default') }}</code> to supply a fallback:

```yaml
sensors:
  - prefect_sensor.sensors.sftp.SFTPSensor:
      name: uploads
      hostname: "{{ env('SFTP_HOST', 'localhost') }}"
      password: "{{ env('SFTP_PASSWORD') }}"
```

### Lifecycle

Each sensor follows the same lifecycle: `setup()` runs once to establish any connections or watchers, `observe()` yields observations until the runner is stopped, and `teardown()` releases resources. Some sensors (for example, [SFTP](./sftp.md)) keep an in-memory fingerprint of the resources they have already seen so they don't re-emit known state.

## Built-in Sensor Packages

* [File System](filesystem.md)
* [SFTP](sftp.md)
* [Kafka](kafka.md)
* [SQL](sql.md)