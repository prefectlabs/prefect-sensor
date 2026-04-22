"""CLI tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from prefect_sensor.cli import app


def test_list_command_prints_sensor_names(sample_sensor_yaml_path: str, capsys) -> None:
    app(
        ["list", "--config", sample_sensor_yaml_path],
        exit_on_error=False,
        result_action="return_value",
    )
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "DummySensor" in out


def test_list_command_missing_file_exits() -> None:
    with pytest.raises(SystemExit) as exc_info:
        app(
            ["list", "--config", str(Path("/nonexistent/sensor.yaml"))],
            exit_on_error=False,
        )
    assert exc_info.value.code == 1
