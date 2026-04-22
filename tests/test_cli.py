"""CLI tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

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


def test_start_command_forwards_summary_interval(sample_sensor_yaml_path: str) -> None:
    with patch(
        "prefect_sensor.cli.SensorManager.start", new_callable=AsyncMock
    ) as start:
        app(
            [
                "start",
                "--config",
                sample_sensor_yaml_path,
                "--summary-interval-seconds",
                "12.5",
            ],
            exit_on_error=False,
            result_action="return_value",
        )

    start.assert_awaited_once()
    assert start.await_args.kwargs["summary_interval_seconds"] == 12.5
