"""YAML loading, env interpolation, and dynamic sensor class import."""

from __future__ import annotations

import importlib
import logging
import os
import re
from pathlib import Path
from typing import Any, Type

import yaml

from prefect_sensor.base import BaseSensor

logger = logging.getLogger("prefect.sensors")

_ENV_PATTERN = re.compile(
    r"\{\{\s*env\(['\"](\w+)['\"]\s*(?:,\s*['\"]([^'\"]*)['\"])?\)\s*\}\}"
)


def _interpolate_env(value: Any) -> Any:
    """
    Recursively resolve {{ env('VAR') }} and {{ env('VAR', 'default') }}
    placeholders in config values.
    """
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            var_name, default = match.group(1), match.group(2)
            result = os.environ.get(var_name, default)
            if result is None:
                msg = (
                    f"Environment variable '{var_name}' is not set "
                    f"and no default provided"
                )
                raise ValueError(msg)
            return result

        return _ENV_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(item) for item in value]
    return value


def _import_class(dotted_path: str) -> Type[Any]:
    """
    Import a class from a fully-qualified dotted path.

    Expects ``module.path.ClassName`` (last segment is the class name).
    """
    parts = dotted_path.rsplit(".", 1)
    if len(parts) != 2:
        msg = f"Invalid import path '{dotted_path}'. Expected 'module.path.ClassName'."
        raise ImportError(msg)

    module_path, class_name = parts
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        msg = (
            f"Could not import module '{module_path}' from path '{dotted_path}'. "
            f"Is the package installed? ({exc})"
        )
        raise ImportError(msg) from exc

    cls = getattr(module, class_name, None)
    if cls is None:
        available = [a for a in dir(module) if not a.startswith("_")]
        msg = (
            f"Module '{module_path}' has no attribute '{class_name}'. "
            f"Available: {available}"
        )
        raise ImportError(msg)

    return cls


def _parse_sensor_entry(entry: dict[Any, Any]) -> BaseSensor:
    """
    Parse one entry from the ``sensors`` list in sensor.yaml.

    Each entry is a single-key dict:
        { "fully.qualified.ClassName": { ...config... } }
    """
    if len(entry) != 1:
        msg = (
            f"Each sensor entry must be a single-key mapping "
            f"(import_path: config), got {len(entry)} keys: {list(entry.keys())}"
        )
        raise ValueError(msg)

    import_path, raw_config = next(iter(entry.items()))
    raw_config = raw_config or {}

    resolved_config = _interpolate_env(raw_config)

    sensor_cls = _import_class(import_path)

    if not (isinstance(sensor_cls, type) and issubclass(sensor_cls, BaseSensor)):
        msg = (
            f"'{import_path}' resolved to {sensor_cls}, "
            f"which is not a BaseSensor subclass."
        )
        raise TypeError(msg)

    config_cls = sensor_cls._config_class
    try:
        config = config_cls(**resolved_config)
    except Exception as exc:
        msg = (
            f"Invalid config for '{import_path}' "
            f"(config class: {config_cls.__name__}): {exc}"
        )
        raise ValueError(msg) from exc

    return sensor_cls(config=config)


def load_sensors_from_yaml(path: str | Path) -> list[BaseSensor]:
    """
    Load and instantiate all sensors from a sensor.yaml file.

    File format::

        sensors:
          - fully.qualified.SensorClass:
              name: my-sensor
              some_config_key: value
              password: "{{ env('SECRET_PASS') }}"
    """
    path = Path(path)
    if not path.exists():
        msg = f"Sensor config not found: {path}"
        raise FileNotFoundError(msg)

    with path.open() as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict) or "sensors" not in raw:
        keys = list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__
        msg = f"sensor.yaml must have a top-level 'sensors' key. Got: {keys}"
        raise ValueError(msg)

    entries = raw["sensors"]
    if not isinstance(entries, list):
        msg = f"'sensors' must be a list, got {type(entries).__name__}"
        raise ValueError(msg)

    sensors: list[BaseSensor] = []
    for i, entry in enumerate(entries):
        try:
            if not isinstance(entry, dict):
                raise ValueError(
                    f"Sensor entry must be a mapping, got {type(entry).__name__}"
                )
            sensor = _parse_sensor_entry(entry)
            sensors.append(sensor)
            logger.info(
                "Loaded sensor [%d]: '%s' (%s)",
                i,
                sensor.config.name,
                type(sensor).__name__,
            )
        except Exception as exc:
            msg = f"Error loading sensor entry {i}: {exc}"
            raise ValueError(msg) from exc

    return sensors
