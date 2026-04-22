"""Example sensor implementations."""

from prefect_sensor.sensors.filesystem import FileSystemSensor
from prefect_sensor.sensors.kafka import KafkaTopicSensor
from prefect_sensor.sensors.sftp import SFTPSensor
from prefect_sensor.sensors.sql import SQLSensor

__all__ = [
    "FileSystemSensor",
    "KafkaTopicSensor",
    "SFTPSensor",
    "SQLSensor",
]
