# prefect-sensor

Modular sensor framework for Prefect event-driven orchestration. Sensors watch external systems and emit Prefect events via `prefect.events.emit_event`, configured by a single YAML file.

- **Modular sensors** — drop-in classes for filesystem, SFTP, Kafka, and SQL, identified by import path so custom sensors slot in alongside the built-ins.
- **Prefect-native events** — observations flow into Prefect Cloud (or a self-hosted Prefect server) through `prefect.events.emit_event` and can drive automations.
- **YAML-first config** — `{{ env('VAR') }}` interpolation for secrets. Run via the CLI, the published `ghcr.io/prefectlabs/prefect-sensor` container image, or as a library.

## Install

```bash
uv sync
```

Add the `dev` group for pytest: `uv sync --group dev`.

## Configure

Create a `sensor.yaml` with sensor class import paths as keys:

```yaml
sensors:
  - prefect_sensor.sensors.filesystem.FileSystemSensor:
      name: home directory sensor
      watch_paths:
        - "~/Desktop"
      ignore_patterns:
        - .DS_Store
```

Use `{{ env('VAR') }}` or `{{ env('VAR', 'default') }}` in any field for secrets. See [docs/docs/sensors/index.md](docs/docs/sensors/index.md) for the common configuration shared across every sensor (`name`, `emit_prefix`, env interpolation) and the per-sensor pages for sensor-specific options.

## Run

### CLI

```bash
prefect-sensor list --config sensor.yaml
prefect-sensor start --config sensor.yaml
prefect-sensor start --config sensor.yaml --summary-interval-seconds 60
```

### Docker

```bash
docker run --rm \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  ghcr.io/prefectlabs/prefect-sensor \
  start --config /config/sensor.yaml
```

See [docs/docs/docker.md](docs/docs/docker.md) for image tags, environment variables (including `PREFECT_API_KEY`), and a `docker-compose` example.

### Library

```python
import asyncio
from prefect_sensor import SensorManager

manager = SensorManager.from_yaml("sensor.yaml")
asyncio.run(manager.start())
```

## Built-in sensors

- [File System](docs/docs/sensors/filesystem.md)
- [SFTP](docs/docs/sensors/sftp.md)
- [Kafka](docs/docs/sensors/kafka.md) *(stub)*
- [SQL](docs/docs/sensors/sql.md)

## Docs

Full documentation lives under [`docs/`](docs/docs/about.md) — start with [About](docs/docs/about.md) and [Getting Started](docs/docs/getting-started.md). To preview the docs site locally:

```bash
cd docs && just dev
```

## Tests

```bash
uv run pytest
```
