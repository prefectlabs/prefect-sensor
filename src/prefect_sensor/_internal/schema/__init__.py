from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from prefect_sensor._internal.schema.config import SensorConfig

__all__ = [
    "SensorConfig",
    "SensorHeartbeat",
    "SensorObservation",
    "SensorState",
]


class SensorState(str, Enum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERRORED = "errored"


@dataclass
class SensorHeartbeat:
    sensor_name: str
    sensor_type: str
    state: SensorState
    events_emitted: int
    errors: int
    last_event_at: Optional[datetime] = None
    last_error: Optional[str] = None
    uptime_seconds: float = 0.0


@dataclass
class SensorObservation:
    """
    A single observation from a sensor — the unit of data that becomes
    a Prefect event.
    """

    event_type: str
    resource_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    occurred: Optional[datetime] = None
