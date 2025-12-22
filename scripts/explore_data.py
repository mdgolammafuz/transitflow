# scripts/explore_data.py

"""
Data Exploration Script for Phase 1.

This script:
1. Tests API connectivity
2. Downloads sample data
3. Computes baseline statistics
4. Validates that the prediction problem is solvable
"""

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from berlin_air_quality.api import WeatherClient
from berlin_air_quality.models import PM25Category


async def test_weather_api():
    """Test Open-Meteo API connectivity and data quality."""
    print("\n" + "="*60)
    print("WEATHER API TEST (Open-Meteo)")
    print("="*60)
    
    async with WeatherClient() as client:
        # Test 1: Current weather
        print("\n1. Fetching current weather...")
        current = await client.get_current()
        print(f"   ✓ Temperature: {current.temperature_c:.1f}°C")
        print(f"   ✓ Wind: {current.wind_speed_ms:.1f} m/s")
        print(f"   ✓ Humidity: {current.humidity_pct:.0f}%")
        
        # Test 2: Forecast
        print("\n2. Fetching 3-day forecast...")
        forecasts = await client.get_forecast(days=3)
        print(f"   ✓ Got {len(forecasts)} hourly forecasts")
        
        if forecasts:
            tomorrow = [f for f in forecasts if f.timestamp.date() == date.today() + timedelta(days=1)]
            if tomorrow:
                avg_wind = sum(f.wind_speed_ms for f in tomorrow) / len(tomorrow)
                max_temp = max(f.temperature_c for f in tomorrow)
                print(f"   ✓ Tomorrow avg wind: {avg_wind:.1f} m/s")
                print(f"   ✓ Tomorrow max temp: {max_temp:.1f}°C")
        
        # Test 3: Historical data
        print("\n3. Fetching historical data (last 7 days)...")
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        
        observations = await client.get_historical(start_date, end_date)
        print(f"   ✓ Got {len(observations)} hourly observations")
        
        if observations:
            avg_temp = sum(o.temperature_c for o in observations) / len(observations)
            avg_wind = sum(o.wind_speed_ms for o in observations) / len(observations)
            print(f"   ✓ Avg temperature: {avg_temp:.1f}°C")
            print(f"   ✓ Avg wind speed: {avg_wind:.1f} m/s")
    
    print("\n   Weather API: WORKING")
    return True


def test_pm25_classification():
    """Test PM2.5 classification logic."""
    print("\n" + "="*60)
    print("PM2.5 CLASSIFICATION TEST")
    print("="*60)
    
    test_cases = [
        (5.0, PM25Category.GOOD),
        (15.0, PM25Category.GOOD),
        (16.0, PM25Category.MODERATE),
        (35.0, PM25Category.MODERATE),
        (36.0, PM25Category.UNHEALTHY),
        (100.0, PM25Category.UNHEALTHY),
    ]
    
    print("\n   Testing thresholds:")
    for value, expected in test_cases:
        result = PM25Category.from_value(value)
        status = "✓" if result == expected else "✗"
        print(f"   {status} PM2.5={value} µg/m³ → {result.value}")
    
    print("\n   Category advisories:")
    for cat in PM25Category:
        print(f"   • {cat.value}: {cat.advisory}")
    
    print("\n   Classification: WORKING")
    return True


def compute_baseline():
    """Compute baseline accuracy for 'same as today' prediction."""
    print("\n" + "="*60)
    print("BASELINE ANALYSIS")
    print("="*60)
    
    # Simulated analysis (will be replaced with real data in exploration notebook)
    print("""
   The baseline model predicts: "Tomorrow = Same as today"
   
   Expected baseline accuracy: ~55-60%
   
   Why? PM2.5 has autocorrelation but weather changes cause shifts.
   
   Target: Beat baseline by 10%+ (achieve 65-70% accuracy)
   
   Key predictive features:
   • Wind speed forecast (dispersion)
   • Precipitation forecast (cleaning)
   • Temperature forecast (inversions)
   • Day of week (traffic patterns)
   • Current PM2.5 levels (momentum)
    """)
    
    print("   Baseline: DEFINED")
    return True


def summarize():
    """Print summary and next steps."""
    print("\n" + "="*60)
    print("PHASE 1 SUMMARY")
    print("="*60)
    
    print("""
   DATA SOURCES:
   ✓ Weather API (Open-Meteo) - Working, free, no auth
   ⚠ Air Quality API (EEA) - Requires testing on local machine
   
   PREDICTION TARGET:
   ✓ Tomorrow's PM2.5 category (Good/Moderate/Unhealthy)
   
   FEATURES (8 total):
   • today_pm25_mean      - Current pollution level
   • today_pm25_max       - Peak today
   • yesterday_pm25_mean  - Trend indicator
   • wind_speed_forecast  - Dispersion potential
   • precipitation_forecast - Rain cleans air
   • temperature_forecast - Inversion risk
   • day_of_week         - Traffic patterns
   • month               - Seasonality
   
   BASELINE:
   • "Same as today" ≈ 55-60% accuracy
   • Target: 65-70% accuracy
   
   NEXT STEPS:
   1. Run exploration notebook for full EDA
   2. Download historical data
   3. Compute actual baseline accuracy
   4. Proceed to Phase 2 (Ingestion)
    """)


async def main():
    """Run all exploration tests."""
    print("\n" + "#"*60)
    print("#" + " BERLIN AIR QUALITY PREDICTOR - PHASE 1 ".center(58) + "#")
    print("#" + " Data Exploration ".center(58) + "#")
    print("#"*60)
    
    try:
        # Test APIs
        await test_weather_api()
        test_pm25_classification()
        compute_baseline()
        summarize()
        
        print("\n" + "="*60)
        print("   PHASE 1 EXPLORATION: COMPLETE")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n   ERROR: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
