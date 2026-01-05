"""
Data models for HSL vehicle telemetry.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator

# Geographic Bounds (Helsinki Region)
MIN_LAT, MAX_LAT = 59.0, 61.0
MIN_LON, MAX_LON = 23.0, 26.0

class VehiclePosition(BaseModel):
    """
    Validated vehicle position event.
    Must match schemas/avro/vehicle_position.avsc
    """

    vehicle_id: int = Field(description="Unique vehicle identifier")
    timestamp: datetime = Field(description="Event timestamp (ISO format)")
    latitude: float = Field(ge=MIN_LAT, le=MAX_LAT, description="WGS84 latitude")
    longitude: float = Field(ge=MIN_LON, le=MAX_LON, description="WGS84 longitude")
    speed_ms: float = Field(ge=0, le=40, description="Speed in m/s")
    heading: int = Field(ge=0, le=360, description="Heading in degrees")
    delay_seconds: int = Field(description="Schedule deviation in seconds")
    
    # FIXED: Changed to bool to match Avro schema recommendation
    door_status: bool = Field(description="True=open, False=closed")
    
    # FIXED: Python int handles large numbers, maps to Avro long
    odometer: Optional[int] = Field(default=None, description="Distance in meters")
    
    next_stop_id: Optional[int] = Field(default=None)
    route_id: Optional[str] = Field(default=None)
    line_id: str = Field(description="Line number")
    direction_id: int = Field(description="1 or 2")
    operator_id: int = Field(description="Operator identifier")
    journey_start: Optional[str] = Field(default=None)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            # Handle Z suffix for ISO 8601
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class RawHSLPayload(BaseModel):
    """
    Raw HSL MQTT payload structure.
    """

    desi: Optional[str] = Field(default=None, description="Line designation")
    dir: Optional[str] = Field(default=None, description="Direction")
    oper: Optional[int] = Field(default=None, description="Operator ID")
    veh: int = Field(description="Vehicle ID")
    tst: str = Field(description="Timestamp ISO")
    tsi: int = Field(description="Timestamp Unix")
    spd: Optional[float] = Field(default=0.0, description="Speed m/s")
    hdg: Optional[int] = Field(default=0, description="Heading degrees")
    lat: float = Field(description="Latitude")
    long: float = Field(description="Longitude")
    acc: Optional[float] = Field(default=None, description="Acceleration")
    dl: Optional[int] = Field(default=0, description="Delay seconds")
    odo: Optional[int] = Field(default=None, description="Odometer")
    drst: Optional[int] = Field(default=0, description="Door status (0/1)")
    oday: Optional[str] = Field(default=None, description="Operating day")
    jrn: Optional[int] = Field(default=None, description="Journey number")
    line: Optional[int] = Field(default=None, description="Line ID internal")
    start: Optional[str] = Field(default=None, description="Journey start time")
    stop: Optional[int] = Field(default=None, description="Next stop ID")
    route: Optional[str] = Field(default=None, description="Route ID")

    def to_vehicle_position(self) -> VehiclePosition:
        """Convert and map types."""
        return VehiclePosition(
            vehicle_id=self.veh,
            timestamp=self.tst,
            latitude=self.lat,
            longitude=self.long,
            speed_ms=self.spd or 0.0,
            heading=self.hdg or 0,
            delay_seconds=self.dl or 0,
            # Conversion int(0/1) -> bool
            door_status=bool(self.drst),
            odometer=self.odo,
            next_stop_id=self.stop,
            route_id=self.route,
            line_id=self.desi or str(self.line) or "unknown",
            direction_id=int(self.dir) if self.dir else 1,
            operator_id=self.oper or 0,
            journey_start=self.start,
        )


class InvalidEvent(BaseModel):
    """DLQ Event Model."""
    raw_payload: str
    error_message: str
    error_field: Optional[str] = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    topic: Optional[str] = None