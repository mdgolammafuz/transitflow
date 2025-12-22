# scripts/ingest_data.py
"""
Ingestion Script - Fetches data from APIs and produces to Kafka.

This script:
1. Fetches PM2.5 data from EEA API
2. Fetches weather data from Open-Meteo API
3. Validates using Pydantic
4. Produces to Kafka topics (valid) or DLQ (invalid)

Run: python scripts/ingest_data.py --mode validate
     python scripts/ingest_data.py --mode kafka
"""

import argparse
import asyncio
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from berlin_air_quality.api import WeatherClient
from berlin_air_quality.ingestion.validators import validate_pm25, validate_weather


async def fetch_weather_data(days_back: int = 1) -> list[dict]:
    """Fetch weather data from Open-Meteo."""
    print(f"\nFETCHING weather data (last {days_back} days)...")
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)
    
    async with WeatherClient() as client:
        observations = await client.get_historical(start_date, end_date)
    
    # Convert to dicts
    records = []
    for obs in observations:
        records.append({
            "timestamp": obs.timestamp,
            "temperature_c": obs.temperature_c,
            "wind_speed_ms": obs.wind_speed_ms,
            "wind_direction_deg": obs.wind_direction_deg,
            "precipitation_mm": obs.precipitation_mm,
            "humidity_pct": obs.humidity_pct,
            "pressure_hpa": obs.pressure_hpa,
            "cloud_cover_pct": obs.cloud_cover_pct,
            "is_forecast": False
        })
    
    print(f"   Fetched {len(records)} weather observations")
    return records


async def fetch_weather_forecast() -> list[dict]:
    """Fetch weather forecast from Open-Meteo."""
    print("\nFETCHING weather forecast...")
    
    async with WeatherClient() as client:
        forecasts = await client.get_forecast(days=3)
    
    records = []
    forecast_time = datetime.now(timezone.utc)
    
    for fc in forecasts:
        records.append({
            "timestamp": fc.timestamp,
            "temperature_c": fc.temperature_c,
            "wind_speed_ms": fc.wind_speed_ms,
            "wind_direction_deg": fc.wind_direction_deg,
            "precipitation_mm": fc.precipitation_mm,
            "humidity_pct": fc.humidity_pct,
            "pressure_hpa": fc.pressure_hpa,
            "cloud_cover_pct": fc.cloud_cover_pct,
            "is_forecast": True,
            "forecast_timestamp": forecast_time
        })
    
    print(f"   Fetched {len(records)} forecast hours")
    return records


def generate_sample_pm25_data() -> list[dict]:
    """
    Generate sample PM2.5 data for testing.
    
    Note: In production, this would fetch from EEA API.
    Using synthetic data here for demonstration.
    """
    print("\nGENERATING sample PM2.5 data...")
    
    import random
    
    stations = [
        ("DEBB001", "Berlin Mitte", 52.5200, 13.4050),
        ("DEBB002", "Berlin Neukölln", 52.4811, 13.4350),
        ("DEBB003", "Berlin Friedrichshain", 52.5159, 13.4538),
        ("DEBB004", "Berlin Charlottenburg", 52.5167, 13.3000),
        ("DEBB005", "Berlin Wedding", 52.5500, 13.3500),
    ]
    
    records = []
    now = datetime.now(timezone.utc)
    
    # Generate last 24 hours of data
    for hours_ago in range(24):
        timestamp = now - timedelta(hours=hours_ago)
        
        for station_id, name, lat, lon in stations:
            # Simulate PM2.5 with some randomness
            base_pm25 = 15 + random.gauss(0, 8)
            
            # Higher values during rush hours (7-9am, 5-7pm)
            hour = timestamp.hour
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                base_pm25 += random.uniform(5, 15)
            
            # Ensure non-negative
            pm25 = max(0, base_pm25)
            
            records.append({
                "station_id": station_id,
                "station_name": name,
                "timestamp": timestamp,
                "value_ugm3": round(pm25, 1),
                "latitude": lat,
                "longitude": lon,
                "is_valid": True
            })
    
    # Add some invalid records for DLQ testing
    invalid_records = [
        # Negative value
        {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": now,
            "value_ugm3": -5.0,
        },
        # Error code
        {
            "station_id": "DEBB002",
            "station_name": "Berlin Neukölln",
            "timestamp": now,
            "value_ugm3": 999,
        },
        # Missing required field
        {
            "station_id": "DEBB003",
            "timestamp": now,
            "value_ugm3": 20.0,
        },
    ]
    
    records.extend(invalid_records)
    
    print(f"   Generated {len(records)} PM2.5 records ({len(invalid_records)} intentionally invalid)")
    return records


def validate_and_report(records: list[dict], record_type: str) -> dict:
    """Validate records and report statistics."""
    valid_count = 0
    invalid_count = 0
    
    validator = validate_pm25 if record_type == "pm25" else validate_weather
    source = "eea" if record_type == "pm25" else "open-meteo"
    
    for record in records:
        valid_record, dlq_record = validator(record, source)
        if valid_record:
            valid_count += 1
        else:
            invalid_count += 1
    
    return {
        "total": len(records),
        "valid": valid_count,
        "invalid": invalid_count,
        "valid_pct": (valid_count / len(records) * 100) if records else 0
    }


async def run_validation_only():
    """Run validation without Kafka (for testing)."""
    print("\n" + "="*60)
    print("VALIDATION MODE (No Kafka)")
    print("="*60)
    
    # Weather data
    weather_records = await fetch_weather_data(days_back=3)
    weather_stats = validate_and_report(weather_records, "weather")
    print(f"\n   Weather: {weather_stats['valid']}/{weather_stats['total']} valid "
          f"({weather_stats['valid_pct']:.1f}%)")
    
    # Forecast
    forecast_records = await fetch_weather_forecast()
    forecast_stats = validate_and_report(forecast_records, "weather")
    print(f"   Forecast: {forecast_stats['valid']}/{forecast_stats['total']} valid "
          f"({forecast_stats['valid_pct']:.1f}%)")
    
    # PM2.5 (sample data)
    pm25_records = generate_sample_pm25_data()
    pm25_stats = validate_and_report(pm25_records, "pm25")
    print(f"   PM2.5: {pm25_stats['valid']}/{pm25_stats['total']} valid "
          f"({pm25_stats['valid_pct']:.1f}%)")
    
    print("\n" + "="*60)
    print("   Validation complete")
    print("="*60)


async def run_kafka_ingestion(use_avro: bool = False):
    """Run full ingestion with Kafka."""
    print("\n" + "="*60)
    print("KAFKA INGESTION MODE")
    print("="*60)
    
    try:
        if use_avro:
            from berlin_air_quality.ingestion import AirQualityProducer
            producer = AirQualityProducer()
        else:
            from berlin_air_quality.ingestion.producer import SimpleProducer
            producer = SimpleProducer()
        
        # Weather data
        weather_records = await fetch_weather_data(days_back=1)
        print(f"\n   Producing {len(weather_records)} weather records...")
        for record in weather_records:
            producer.produce_weather(record)
        
        # Forecast
        forecast_records = await fetch_weather_forecast()
        print(f"   Producing {len(forecast_records)} forecast records...")
        for record in forecast_records:
            producer.produce_weather(record)
        
        # PM2.5 (sample)
        pm25_records = generate_sample_pm25_data()
        print(f"   Producing {len(pm25_records)} PM2.5 records...")
        for record in pm25_records:
            producer.produce_pm25(record)
        
        # Flush
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            print(f"   WARNING: {remaining} messages not delivered")
        
        # Stats
        stats = producer.get_stats()
        print("\n   Production Statistics:")
        print(f"   PM2.5 produced: {stats['pm25_produced']}")
        print(f"   PM2.5 failed (to DLQ): {stats['pm25_failed']}")
        print(f"   Weather produced: {stats['weather_produced']}")
        print(f"   Weather failed (to DLQ): {stats['weather_failed']}")
        print(f"   DLQ records: {stats['dlq_produced']}")
        
        print("\n" + "="*60)
        print("   Kafka ingestion complete")
        print("="*60)
        
    except ImportError as e:
        print(f"\n   Kafka libraries not installed: {e}")
        print("   Run: pip install confluent-kafka")
        print("   Falling back to validation-only mode...")
        await run_validation_only()


def main():
    parser = argparse.ArgumentParser(
        description="Ingest air quality and weather data"
    )
    parser.add_argument(
        "--mode",
        choices=["validate", "kafka", "kafka-avro"],
        default="validate",
        help="Ingestion mode"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Days of historical data to fetch"
    )
    
    args = parser.parse_args()
    
    print("\n" + "#"*60)
    print("#" + " BERLIN AIR QUALITY - DATA INGESTION ".center(58) + "#")
    print("#"*60)
    
    if args.mode == "validate":
        asyncio.run(run_validation_only())
    elif args.mode == "kafka":
        asyncio.run(run_kafka_ingestion(use_avro=False))
    elif args.mode == "kafka-avro":
        asyncio.run(run_kafka_ingestion(use_avro=True))


if __name__ == "__main__":
    main()
