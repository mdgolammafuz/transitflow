"""
Data models for HSL vehicle telemetry.
Honest: Does not hardcode zeros for missing sensor data.
Resilient: Explicitly casts IDs to str to satisfy Pydantic/Warehouse contract.
Aligned: Enforces UTC-awareness at the point of ingestion.
Hardened: Gracefully handles GPS dropouts (null lat/long) from the HSL feed.
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

    # Made Optional to prevent 0.0 defaults
    speed_ms: Optional[float] = Field(default=None, ge=0, le=40, description="Speed in m/s")
    heading: Optional[int] = Field(default=None, ge=0, le=360, description="Heading in degrees")
    delay_seconds: Optional[int] = Field(default=None, ge=-600, le=7200, description="Schedule deviation")
    door_status: Optional[int] = Field(default=None, ge=0, le=1, description="0=closed, 1=open")
    
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
    Hardened: lat/long marked as Optional to handle HSL sensor dropouts.
    """

    desi: Optional[str] = Field(default=None)
    dir: Optional[str] = Field(default=None)
    oper: Optional[int] = Field(default=None)
    veh: int = Field(description="Raw Vehicle ID")
    tst: str = Field(description="Timestamp ISO")
    tsi: int = Field(description="Timestamp Unix")
    spd: Optional[float] = Field(default=None)
    hdg: Optional[int] = Field(default=None)
    
    # Now Optional to allow Pydantic to accept 'null' values from HSL
    lat: Optional[float] = Field(default=None, description="Latitude")
    long: Optional[float] = Field(default=None, description="Longitude")
    
    dl: Optional[int] = Field(default=None)
    drst: Optional[int] = Field(default=None)
    line: Optional[int] = Field(default=None)
    start: Optional[str] = Field(default=None)
    stop: Optional[int] = Field(default=None)
    route: Optional[str] = Field(default=None)

    def to_vehicle_position(self) -> VehiclePosition:
        """
        Flatten and validate raw HSL payload into the standardized contract.
        
        STRICT AUDIT POINTS:
        1. GPS INTEGRITY: Raises ValueError if lat/long are None. This is the only 
           legal way to trigger the DLQ (Dead Letter Queue) in the bridge.
        2. NO IDENTITY DRIFT: Explicitly casts IDs to str to satisfy Pydantic.
        3. HONEST MAPPING: Removed hardcoded zeros. Sensor data defaults to None.
        """
        
        # Guard 1: Without coordinates, the record is useless for Feature Engineering.
        if self.lat is None or self.long is None:
            raise ValueError(f"GPS dropout detected for vehicle {self.veh}")

        return VehiclePosition(
            # Casting to str resolves 'Input should be string' logs
            vehicle_id=str(self.veh),
            timestamp=self.tst,
            latitude=self.lat,
            longitude=self.long,
            
            # If raw is None, passed as None.
            speed_ms=self.spd,
            heading=self.hdg,
            delay_seconds=self.dl,
            door_status=self.drst,
            
            # String cast ensures schema consistency across the lakehouse
            next_stop_id=str(self.stop) if self.stop is not None else None,
            
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