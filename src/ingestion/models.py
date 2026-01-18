"""
Data models for HSL vehicle telemetry.
Aligned: Enforces UTC-awareness at the point of ingestion.
Robustness: Standardizes IDs as strings to match Lakehouse schema.
Consistency: Maintains 'next_stop_id' to match HSL spec and prevent downstream breakage.
"""

from datetime import datetime, timezone
from typing import Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class VehiclePosition(BaseModel):
    """
    Validated vehicle position event.
    Standardized for the TransitFlow Lakehouse.
    """

    # Using Strings for IDs to prevent numeric drift/overflow in Spark/Delta
    vehicle_id: str = Field(description="Unique vehicle identifier")
    timestamp: datetime = Field(description="Event timestamp (Forced UTC)")
    latitude: float = Field(ge=59.0, le=61.0, description="WGS84 latitude")
    longitude: float = Field(ge=23.0, le=26.0, description="WGS84 longitude")
    speed_ms: float = Field(ge=0, le=40, description="Speed in m/s")
    heading: int = Field(ge=0, le=360, description="Heading in degrees")
    delay_seconds: int = Field(ge=-600, le=7200, description="Schedule deviation")
    door_status: int = Field(ge=0, le=1, description="0=closed, 1=open")
    odometer: Optional[int] = Field(default=None, description="Distance in meters")

    # Kept as next_stop_id to maintain consistency with HSL and existing code
    next_stop_id: Optional[str] = Field(default=None, description="Next stop identifier")

    route_id: Optional[str] = Field(default=None, description="Route identifier")
    line_id: str = Field(description="Line number (e.g., '600')")
    direction_id: int = Field(ge=1, le=2, description="1 or 2")
    operator_id: int = Field(description="Operator identifier")
    journey_start: Optional[str] = Field(default=None, description="Scheduled start time")

    # Computed field for Flink/Spark Watermarking
    event_time_ms: int = Field(default=0, description="Unix timestamp in UTC milliseconds")

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, v: Union[str, datetime]) -> datetime:
        """
        Force UTC awareness immediately.
        Prevents Python from guessing the timezone based on the host OS (IST/CET).
        """
        if isinstance(v, str):
            # Replace 'Z' with +00:00 for ISO compatibility
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            dt = v

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @model_validator(mode="after")
    def compute_ms(self) -> "VehiclePosition":
        """Deterministic millisecond calculation for downstream event-time processing."""
        self.event_time_ms = int(self.timestamp.timestamp() * 1000)
        return self


class RawHSLPayload(BaseModel):
    """
    Raw HSL MQTT payload structure wrapper (VP object).
    """

    desi: Optional[str] = Field(default=None)
    dir: Optional[str] = Field(default=None)
    oper: Optional[int] = Field(default=None)
    veh: int = Field(description="Raw Vehicle ID")
    tst: str = Field(description="Timestamp ISO")
    tsi: int = Field(description="Timestamp Unix")
    spd: Optional[float] = Field(default=0.0)
    hdg: Optional[int] = Field(default=0)
    lat: float = Field(description="Latitude")
    long: float = Field(description="Longitude")
    dl: Optional[int] = Field(default=0)
    drst: Optional[int] = Field(default=0)
    line: Optional[int] = Field(default=None)
    start: Optional[str] = Field(default=None)
    stop: Optional[int] = Field(default=None)
    route: Optional[str] = Field(default=None)

    def to_vehicle_position(self) -> VehiclePosition:
        """Flatten and validate raw payload into the clean contract."""
        return VehiclePosition(
            vehicle_id=str(self.veh),
            timestamp=self.tst,
            latitude=self.lat,
            longitude=self.long,
            speed_ms=self.spd or 0.0,
            heading=self.hdg or 0,
            delay_seconds=self.dl or 0,
            door_status=self.drst or 0,
            next_stop_id=str(self.stop) if self.stop else None,
            route_id=self.route,
            line_id=self.desi or str(self.line) or "unknown",
            direction_id=int(self.dir) if self.dir else 1,
            operator_id=self.oper or 0,
            journey_start=self.start,
        )


class InvalidEvent(BaseModel):
    """
    Schema for the Dead Letter Queue (DLQ).
    """

    raw_payload: str
    error_message: str
    error_field: Optional[str] = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    topic: Optional[str] = None
