"""Modular sensor framework for Prefect event-driven orchestration."""

from prefect_sensor.base import BaseSensor, StatefulSensorMixin
from prefect_sensor.loader import load_sensors_from_yaml
from prefect_sensor.manager import SensorManager
from prefect_sensor._internal.schema import (
    SensorHeartbeat,
    SensorObservation,
    SensorState,
)
from prefect_sensor._internal.schema.config import SensorConfig
from prefect_sensor.sensors import (
    FileSystemSensor,
    KafkaTopicSensor,
    SFTPSensor,
    SQLSensor,
)

__all__ = [
    "BaseSensor",
    "FileSystemSensor",
    "KafkaTopicSensor",
    "SFTPSensor",
    "SQLSensor",
    "SensorConfig",
    "SensorHeartbeat",
    "SensorManager",
    "SensorObservation",
    "SensorState",
    "StatefulSensorMixin",
    "load_sensors_from_yaml",
]
