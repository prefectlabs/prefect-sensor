---
title: File System Sensor
---

# File System Sensor

`prefect_sensor.sensors.filesystem.FileSystemSensor` watches one or more local paths and emits an event whenever a file or directory changes. It is backed by the [`watchdog`](https://python-watchdog.readthedocs.io/) library, so changes are delivered through native operating-system notifications rather than by polling.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `watch_paths` | `list[str]` | *required* | Paths to monitor. `~` is expanded and missing paths are skipped with a warning. |
| `recursive` | `bool` | `true` | Watch subdirectories of each `watch_paths` entry. |
| `events` | `list[str]` | `["created", "modified", "deleted", "moved"]` | Which event kinds to emit. Any subset of the default list. |
| `patterns` | `list[str]` | `["*"]` | Glob patterns a path must match to produce an event. |
| `ignore_patterns` | `list[str]` | `null` | Glob patterns whose matches are dropped. |
| `ignore_directories` | `bool` | `true` | When `true`, directory-level events are suppressed. |
| `case_sensitive` | `bool` | `false` | Whether `patterns` and `ignore_patterns` match case-sensitively. |
| `state_file` | `str \| null` | `null` | Path to a JSON file used to persist the highest `mtime` seen by the sensor. On startup, if a prior HWM is loaded the sensor performs a catch-up scan of `watch_paths` and emits `file.created` (with `payload.catchup: true`) for any file newer than the HWM that watchdog would have missed while the sensor was down. |
| `emit_prefix` | `str` | `"sensor.filesystem"` | Prefix prepended to every emitted event type. |

## Example

```yaml
sensors:
  - prefect_sensor.sensors.filesystem.FileSystemSensor:
      name: home directory sensor
      watch_paths:
        - "~/Desktop"
      ignore_patterns:
        - .DS_Store
```

See [Getting Started](../getting-started.md) for how to run a config.

## Events emitted

Event types take the form `{emit_prefix}.{kind}.{action}`, where `kind` is `file` or `directory` and `action` is one of `created`, `modified`, `deleted`, or `moved`. With the default `emit_prefix`, the resulting events are:

- `sensor.filesystem.file.created`
- `sensor.filesystem.file.modified`
- `sensor.filesystem.file.deleted`
- `sensor.filesystem.file.moved`

Directory variants (`sensor.filesystem.directory.*`) are emitted only when `ignore_directories: false`.

Each observation carries:

- **Resource ID** — `filesystem:{path}`
- **Payload** — `path`, `kind`, `is_directory`, `watch_root`. Move events additionally include `source_path` and `destination_path`. Catch-up events (see below) additionally include `catchup: true`.

## Runnable filesystem example

The repository includes a complete
[Docker Compose example](https://github.com/prefectlabs/prefect-sensor/tree/main/examples/filesystem)
with `cgen` writing generated data into a host directory bind-mounted into a
locally built sensor container. It targets Prefect Cloud and requires
`PREFECT_API_URL` and `PREFECT_API_KEY`. The generated log demonstrates created
and modified events automatically; creating, renaming, or deleting another file
in the shared directory demonstrates the remaining lifecycle events.

## State persistence

When `state_file` is set, the sensor persists the highest `mtime` (the HWM) it has observed. After each emitted watchdog event the HWM advances to "now" and the file is rewritten atomically; teardown also flushes. On startup:

- If no prior state exists, the HWM is seeded to the current time. No catch-up scan runs and pre-existing files are not replayed.
- If a prior HWM is loaded, the sensor scans `watch_paths` (honouring `recursive`, `patterns`, `ignore_patterns`, `case_sensitive`) and emits a synthetic `file.created` event for every regular file whose `mtime` exceeds the HWM. These catch-up observations include `catchup: true` in their payload so downstream consumers can distinguish replay from live events.

The HWM tracks process-wall-clock time, not file mtime — it is conservative against missed events but does not deduplicate files that existed before the sensor was first run with a `state_file`.
