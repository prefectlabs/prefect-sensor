"""Cyclopts CLI: start sensors or list configured sensors."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

from prefect_sensor.manager import SensorManager

app = App(
    name="prefect-sensor",
    help="Prefect Sensor Process — observe external systems and emit Prefect events.",
)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s │ %(name)-30s │ %(levelname)-7s │ %(message)s",
    )


def _register_signal_handlers(manager: SensorManager) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        asyncio.create_task(manager.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # Windows: no add_signal_handler for SIGTERM in some contexts
            pass


async def _run_start(config: Path) -> None:
    _configure_logging()
    manager = SensorManager.from_yaml(config)

    print("\nPrefect Sensor Process")
    print(f"   Config: {config}")
    print(f"   {len(manager.sensors)} sensor(s) loaded:\n")
    for s in manager.sensors:
        fq = f"{type(s).__module__}.{type(s).__qualname__}"
        print(f"   • {s.config.name:30s}  ({fq})")
    print()

    _register_signal_handlers(manager)
    await manager.start()

    print("\nFinal status:")
    print(manager.summary())


@app.command
def start(
    config: Annotated[
        Path,
        Parameter(help="Path to sensor.yaml", name=("--config", "-c")),
    ] = Path("sensor.yaml"),
) -> None:
    """Run all sensors from the YAML config until interrupted."""
    try:
        asyncio.run(_run_start(config))
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc


@app.command(name="list")
def list_sensors(
    config: Annotated[
        Path,
        Parameter(help="Path to sensor.yaml", name=("--config", "-c")),
    ] = Path("sensor.yaml"),
) -> None:
    """Print configured sensors without running them."""
    try:
        manager = SensorManager.from_yaml(config)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Config: {config}")
    print(f"{len(manager.sensors)} sensor(s):\n")
    for s in manager.sensors:
        fq = f"{type(s).__module__}.{type(s).__qualname__}"
        print(f"  • {s.config.name:30s}  ({fq})")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
