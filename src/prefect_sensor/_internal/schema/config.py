from typing import Dict, Optional

from pydantic import BaseModel, Field, ConfigDict


class SensorConfig(BaseModel):
    """
    Base configuration every sensor shares.

    Subclasses extend this with their own fields. Pydantic gives us
    validation, env-var interpolation, and serialization — matching
    the Block config pattern.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique human-readable sensor name")
    labels: Dict[str, str] = Field(default_factory=dict)
    emit_prefix: str = Field(default="sensor")
    error_backoff_seconds: float = Field(default=5.0, ge=0)
    max_consecutive_errors: int = Field(default=10, ge=1)
    state_file: Optional[str] = Field(
        default=None,
        description="Path to a JSON file used to persist sensor state across restarts.",
    )
