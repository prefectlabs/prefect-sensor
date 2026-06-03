"""Pytest configuration."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def sample_sensor_yaml_path() -> str:
    return str(Path(__file__).parent / "fixtures" / "sample_sensor.yaml")
