---
title: SQL Sensor
---

# SQL Sensor

`prefect_sensor.sensors.sql.SQLSensor` polls a SQL table for new rows by tracking a monotonically rising column. It is built on async [SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html); on each tick it runs the configured `query` with the current high-water-mark (HWM) bound to `:hwm`, emits one observation per returned row, and advances the HWM to the row's `tracking_column` value.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `connection_string` | `str` | *required* | Async SQLAlchemy URL (e.g. `sqlite+aiosqlite:///./orders.db`, `postgresql+asyncpg://user:pass@host/db`). |
| `query` | `str` | *required* | SELECT statement; must include the `:hwm` bind parameter and must return rows in non-decreasing order of `tracking_column`. |
| `tracking_column` | `str` | *required* | Column in the SELECT list whose value advances the HWM. |
| `tracking_type` | `"integer" \| "timestamp"` | `"integer"` | Type of the tracking column. `timestamp` coerces naive values to UTC. |
| `start_value` | `str \| null` | `null` | Initial HWM as a Python expression evaluated against a restricted namespace exposing `datetime`, `timezone`, and `timedelta`. E.g. `"1000"` or `"datetime(2024, 1, 1, tzinfo=timezone.utc)"`. |
| `emit_existing` | `bool` | `false` | If true, seed the HWM at a type-appropriate floor (`0` for `integer`, `datetime.min` UTC for `timestamp`) so the first poll emits every row currently in the table. |
| `state_file` | `str \| null` | `null` | Path to a JSON file used to persist the HWM atomically after each non-empty poll. When present and existing on `setup()`, it overrides `start_value` / `emit_existing`. |
| `poll_interval_seconds` | `float` | `15.0` | Seconds between polls. Must be ≥ 0. |
| `emit_prefix` | `str` | `"sensor.sql"` | Prefix prepended to every emitted event type. |

## Example

```yaml
sensors:
  - prefect_sensor.sensors.sql.SQLSensor:
      name: orders-watcher
      connection_string: "sqlite+aiosqlite:///./orders.db"
      query: "SELECT id, customer, status FROM orders WHERE id > :hwm ORDER BY id"
      tracking_column: id
      tracking_type: integer
      emit_existing: true
      state_file: /var/lib/prefect-sensor/orders.json
      poll_interval_seconds: 5
```

See [Getting Started](../getting-started.md) for how to run a config.

## How tracking works

**`:hwm` bind parameter.** Every poll executes the configured `query` with the current HWM bound to `:hwm`. The query must include `:hwm` — config validation fails otherwise. A canonical shape is `WHERE id > :hwm ORDER BY id`. After all rows are emitted, the HWM advances to the last row's `tracking_column` value, so subsequent polls only return new rows.

**Seeding the HWM.** On first run (no state file yet), exactly one of `start_value` or `emit_existing` is required. `start_value` is a Python expression evaluated against a restricted namespace exposing `datetime`, `timezone`, and `timedelta` — useful for "start watching from the beginning of 2024" without inlining a magic number. `emit_existing: true` seeds the HWM at a type-appropriate floor (`0` for `integer`, `datetime.min` UTC for `timestamp`) so every row currently in the table is emitted on the first poll.

**State persistence.** When `state_file` is set, the HWM is written atomically (temp file + `os.replace`) after every non-empty poll and on teardown, so restarts resume without replaying already-emitted rows. If the file exists at startup, it overrides `start_value` / `emit_existing`.

## Events emitted

With the default `emit_prefix`, the sensor emits `sensor.sql.row.detected` — one event per returned row.

Each observation carries:

- **Resource ID** — `sql:{tracking_column}:{value}`, where `value` is the row's tracking-column value.
- **Payload** — the full row as a dict (keys are the SELECT list's column names). For `tracking_type: timestamp`, naive datetime values returned by the driver are normalised to UTC before being placed in the payload and used to advance the HWM.

## Drivers

`sqlalchemy[asyncio]` and `aiosqlite` ship with the project, so SQLite-async URLs (`sqlite+aiosqlite:///...`) work out of the box. To use Postgres or another backend, install the matching async driver alongside `prefect-sensor` — e.g. `pip install asyncpg` for `postgresql+asyncpg://...`.
