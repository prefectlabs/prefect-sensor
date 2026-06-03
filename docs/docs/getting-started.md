---
title: Getting Started
---

# Getting Started

Prefect Sensor watches external systems and emits Prefect events. This page walks through installing the package, writing a minimal configuration, and running it from the CLI or as a library.

## Install

```bash
uv sync
```

For development dependencies (pytest):

```bash
uv sync --group dev
```

## Write a config

Sensors are configured in a YAML file with a top-level `sensors` key. Each entry uses the sensor class's import path as the key, with that sensor's configuration nested underneath.

```yaml
sensors:
  - prefect_sensor.sensors.filesystem.FileSystemSensor:
      name: home directory sensor
      watch_paths:
        - "~/Desktop"
      ignore_patterns:
        - .DS_Store
```

Use <code v-pre>{{ env('VAR') }}</code> or <code v-pre>{{ env('VAR', 'default') }}</code> to interpolate environment variables — useful for credentials and other secrets:

```yaml
sensors:
  - prefect_sensor.sensors.sftp.SFTPSensor:
      name: uploads
      hostname: sftp.example.com
      username: "{{ env('SFTP_USER') }}"
      password: "{{ env('SFTP_PASSWORD') }}"
```

## Run via CLI

List the sensors a config file would start:

```bash
prefect-sensor list --config sensor.yaml
```

Start them:

```bash
prefect-sensor start --config sensor.yaml
```

The runner prints a summary of emitted events on an interval. To change the cadence:

```bash
prefect-sensor start --config sensor.yaml --summary-interval-seconds 60
```

## Run with Docker

A pre-built image is published at `ghcr.io/prefectlabs/prefect-sensor`. Mount your config and run:

```bash
docker run --rm \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  ghcr.io/prefectlabs/prefect-sensor \
  start --config /config/sensor.yaml
```

See the [Docker guide](./docker.md) for image tags, environment variables (including `PREFECT_API_KEY`), and a `docker-compose` example.

## Use as a library

The same config can be loaded programmatically:

```python
import asyncio
from prefect_sensor import SensorManager

manager = SensorManager.from_yaml("sensor.yaml")
asyncio.run(manager.start())
```

## Next steps

See the [Sensors](./sensors/index.md) overview for the list of built-in sensors and their configuration options.
