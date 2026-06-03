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
- **Payload** — `path`, `kind`, `is_directory`, `watch_root`. Move events additionally include `source_path` and `destination_path`.
