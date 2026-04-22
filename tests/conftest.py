"""Pytest configuration."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_sensor_yaml_path() -> str:
    from pathlib import Path

    return str(Path(__file__).parent / "fixtures" / "sample_sensor.yaml")
