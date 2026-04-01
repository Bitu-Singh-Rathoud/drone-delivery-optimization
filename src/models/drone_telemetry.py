"""Drone telemetry data model."""

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Optional


@dataclass
class DroneTelemetry:
    """Represents a single telemetry snapshot from a drone."""

    drone_id: str
    latitude: float
    longitude: float
    altitude: float        # metres above sea level
    speed: float           # m/s
    heading: float         # degrees (0–360)
    battery_level: float   # percentage 0–100
    status: str            # 'en_route', 'hovering', 'returning', 'landing', 'emergency'
    timestamp: float = field(default_factory=time.time)
    destination_lat: Optional[float] = None
    destination_lon: Optional[float] = None
    payload_weight: float = 0.0   # kg

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "DroneTelemetry":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_json(cls, json_str: str) -> "DroneTelemetry":
        return cls.from_dict(json.loads(json_str))

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def is_low_battery(self, threshold: float = 20.0) -> bool:
        return self.battery_level < threshold

    def is_emergency(self) -> bool:
        return self.status == "emergency"
