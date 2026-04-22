# prefect-sensor

Modular sensor framework for Prefect event-driven orchestration. Sensors watch external systems and emit Prefect events via `prefect.events.emit_event`.

## Install

```bash
uv sync
```

Development dependencies (pytest): `uv sync --group dev`.

Filesystem sensors use `watchdog` for native filesystem events and are installed with the project dependencies.

## Configure

Create a `sensor.yaml` with import paths as keys (see tests/fixtures for examples):

```yaml
sensors:
  - tests.helpers.DummySensor:
      name: my-sensor
      n: 3
```

Use `{{ env('VAR') }}` or `{{ env('VAR', 'default') }}` for secrets.

## CLI

```bash
prefect-sensor list --config sensor.yaml
prefect-sensor start --config sensor.yaml
prefect-sensor start --config sensor.yaml --summary-interval-seconds 60
```

## Library

```python
from prefect_sensor import SensorManager

manager = SensorManager.from_yaml("sensor.yaml")
# asyncio.run(manager.start())
```

## Tests

```bash
uv run pytest
```
