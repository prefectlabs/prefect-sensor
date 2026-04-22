"""Tests for YAML loading, env interpolation, and dynamic imports."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from prefect_sensor.base import BaseSensor
from prefect_sensor.loader import (
    _import_class,
    _interpolate_env,
    _parse_sensor_entry,
    load_sensors_from_yaml,
)


def test_interpolate_env_plain_string() -> None:
    assert _interpolate_env("hello") == "hello"


def test_interpolate_env_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TEST_VAR", "resolved")
    assert _interpolate_env("{{ env('MY_TEST_VAR') }}") == "resolved"


def test_interpolate_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MY_MISSING_XYZ", raising=False)
    assert _interpolate_env("{{ env('MY_MISSING_XYZ', 'fallback') }}") == "fallback"


def test_interpolate_env_missing_raises() -> None:
    with pytest.raises(ValueError, match="MY_MISSING_NO_DEFAULT"):
        _interpolate_env("{{ env('MY_MISSING_NO_DEFAULT') }}")


def test_interpolate_env_nested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("A", "1")
    monkeypatch.setenv("B", "2")
    data = {
        "x": [{"y": "{{ env('A') }}"}],
        "z": "{{ env('B') }}",
    }
    out = _interpolate_env(data)
    assert out == {"x": [{"y": "1"}], "z": "2"}


def test_import_class_pathlib_path() -> None:
    cls = _import_class("pathlib.Path")
    assert cls is Path


def test_import_class_invalid_format() -> None:
    with pytest.raises(ImportError, match="Invalid import path"):
        _import_class("NoDot")


def test_import_class_missing_module() -> None:
    with pytest.raises(ImportError, match="Could not import module"):
        _import_class("definitely_missing_module_xyz.SomeClass")


def test_import_class_missing_attribute() -> None:
    with pytest.raises(ImportError, match="has no attribute"):
        _import_class("pathlib.NotARealClass")


def test_parse_sensor_entry_not_base_sensor() -> None:
    entry = {"pathlib.Path": {"name": "bad"}}
    with pytest.raises(TypeError, match="not a BaseSensor subclass"):
        _parse_sensor_entry(entry)


def test_parse_sensor_entry_wrong_key_count() -> None:
    entry = {"a": {}, "b": {}}
    with pytest.raises(ValueError, match="single-key"):
        _parse_sensor_entry(entry)


def test_load_sensors_from_yaml(sample_sensor_yaml_path: str) -> None:
    sensors = load_sensors_from_yaml(sample_sensor_yaml_path)
    assert len(sensors) == 2
    assert {s.config.name for s in sensors} == {"alpha", "beta"}
    assert all(isinstance(s, BaseSensor) for s in sensors)


def test_load_sensors_from_yaml_env_interpolation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PREFECT_SENSOR_TEST_SECRET", "sekrit")
    path = Path(__file__).parent / "fixtures" / "env_sensor.yaml"
    sensors = load_sensors_from_yaml(path)
    assert len(sensors) == 1
    assert sensors[0].config.labels["secret"] == "sekrit"


def test_load_sensors_from_yaml_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        load_sensors_from_yaml("/nonexistent/sensor.yaml")


def test_load_sensors_from_yaml_bad_top_level(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.dump({"not_sensors": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="sensors"):
        load_sensors_from_yaml(p)


def test_load_sensors_from_yaml_sensors_not_list(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.dump({"sensors": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="list"):
        load_sensors_from_yaml(p)


def test_load_sensors_from_yaml_entry_not_mapping(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.dump({"sensors": ["not-a-mapping"]}), encoding="utf-8")
    with pytest.raises(ValueError, match="mapping"):
        load_sensors_from_yaml(p)
