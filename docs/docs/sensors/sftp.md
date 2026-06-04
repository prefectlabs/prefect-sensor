---
title: SFTP Sensor
---

# SFTP Sensor

`prefect_sensor.sensors.sftp.SFTPSensor` polls one or more remote directories over SFTP and emits events when files appear, change, or disappear. It uses [Paramiko](https://www.paramiko.org/) for the SSH connection and tracks file size and modification time to detect changes between polls.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `hostname` | `str` | *required* | SFTP server host. |
| `port` | `int` | `22` | SFTP port. |
| `username` | `str` | `""` | SFTP username. Empty means no username is passed. |
| `password` | `str` | `""` | Password. Prefer <code v-pre>{{ env('SFTP_PASSWORD') }}</code> over inlining secrets. |
| `private_key_path` | `str` | `""` | Path to a private key file to use for authentication. |
| `allow_agent` | `bool` | `true` | Allow Paramiko to use a running SSH agent. |
| `look_for_keys` | `bool` | `true` | Auto-discover keys in `~/.ssh`. |
| `remote_directories` | `list[str]` | `["/upload"]` | Remote directories to poll. Missing directories log a warning and are skipped. |
| `poll_interval_seconds` | `float` | `30.0` | Seconds between polls. |
| `state_file` | `str \| null` | `null` | Path to a JSON file used to persist the last-seen file `mtime` across restarts. When present and existing on `setup()`, files with `mtime <= state` are silently treated as already-known on the first poll so `file.appeared` is not refired. |
| `emit_prefix` | `str` | `"sensor.sftp"` | Prefix prepended to every emitted event type. |

## Example

```yaml
sensors:
  - prefect_sensor.sensors.sftp.SFTPSensor:
      name: sftp
      hostname: localhost
      port: 2222
      username: sftp
      password: "{{ env('SFTP_PASSWORD') }}"
      remote_directories:
        - /upload
```

See [Getting Started](../getting-started.md) for how to run a config.

## Events emitted

With the default `emit_prefix`, the sensor emits:

- `sensor.sftp.file.appeared` — a regular file is seen for the first time.
- `sensor.sftp.file.changed` — size or modification time has changed since the previous poll.
- `sensor.sftp.file.removed` — a previously-seen file is no longer present.

Each observation carries:

- **Resource ID** — `sftp:{hostname}:{remote_path}`
- **Payload** — `hostname` and `remote_path`. `appeared` and `changed` events additionally include `size` and `mtime`.

The sensor maintains an in-memory map of remote paths to size/mtime, so unchanged files are not re-emitted on every poll.

## State persistence

By default, restarting the sensor resets the in-memory map and any existing files will be re-emitted as `file.appeared`. Set `state_file` to a writable path to persist the highest `mtime` the sensor has observed. After each poll and on teardown the HWM is written atomically (temp file + `os.replace`). On the next startup, files with `mtime <= HWM` are silently added to the in-memory map without refiring `file.appeared`; only files newer than the HWM produce events. `file.changed` and `file.removed` semantics are unaffected.
