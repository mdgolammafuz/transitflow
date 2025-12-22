"""
Tests for API clients.

Run with: pytest tests/test_api_clients.py -v
"""

import pytest
from datetime import date, datetime, timedelta

from berlin_air_quality.api import WeatherClient
from berlin_air_quality.api.weather_client import WeatherObservation, WeatherForecast
from berlin_air_quality.models import PM25Category


# =============================================================================
# Weather Client Tests
# =============================================================================

class TestWeatherClient:
    """Tests for Open-Meteo weather client."""
    
    @pytest.mark.asyncio
    async def test_get_forecast(self):
        """Test fetching weather forecast."""
        async with WeatherClient() as client:
            forecasts = await client.get_forecast(days=2)
        
        assert len(forecasts) > 0
        assert isinstance(forecasts[0], WeatherForecast)
        
        # Check forecast has required fields
        fc = forecasts[0]
        assert fc.temperature_c is not None
        assert fc.wind_speed_ms >= 0
        assert 0 <= fc.humidity_pct <= 100
    
    @pytest.mark.asyncio
    async def test_get_historical(self):
        """Test fetching historical weather."""
        # Get data from 7 days ago
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=3)
        
        async with WeatherClient() as client:
            observations = await client.get_historical(start_date, end_date)
        
        assert len(observations) > 0
        assert isinstance(observations[0], WeatherObservation)
        
        # Check observation is within date range
        obs = observations[0]
        assert start_date <= obs.timestamp.date() <= end_date
    
    @pytest.mark.asyncio
    async def test_get_current(self):
        """Test fetching current weather."""
        async with WeatherClient() as client:
            current = await client.get_current()
        
        assert isinstance(current, WeatherObservation)
        assert -60 <= current.temperature_c <= 60
        assert current.wind_speed_ms >= 0


# =============================================================================
# PM25 Category Tests
# =============================================================================

class TestPM25Category:
    """Tests for PM2.5 categorization."""
    
    def test_good_category(self):
        """Test Good category threshold."""
        assert PM25Category.from_value(0) == PM25Category.GOOD
        assert PM25Category.from_value(10) == PM25Category.GOOD
        assert PM25Category.from_value(15) == PM25Category.GOOD
    
    def test_moderate_category(self):
        """Test Moderate category threshold."""
        assert PM25Category.from_value(16) == PM25Category.MODERATE
        assert PM25Category.from_value(25) == PM25Category.MODERATE
        assert PM25Category.from_value(35) == PM25Category.MODERATE
    
    def test_unhealthy_category(self):
        """Test Unhealthy category threshold."""
        assert PM25Category.from_value(36) == PM25Category.UNHEALTHY
        assert PM25Category.from_value(50) == PM25Category.UNHEALTHY
        assert PM25Category.from_value(100) == PM25Category.UNHEALTHY
    
    def test_numeric_encoding(self):
        """Test numeric encoding for ML."""
        assert PM25Category.GOOD.numeric == 0
        assert PM25Category.MODERATE.numeric == 1
        assert PM25Category.UNHEALTHY.numeric == 2
    
    def test_advisory_text(self):
        """Test advisory text exists."""
        for category in PM25Category:
            assert len(category.advisory) > 0


# =============================================================================
# Integration Test (requires network)
# =============================================================================

@pytest.mark.integration
class TestIntegration:
    """Integration tests requiring network access."""
    
    @pytest.mark.asyncio
    async def test_weather_api_available(self):
        """Test that Open-Meteo API is accessible."""
        async with WeatherClient() as client:
            forecasts = await client.get_forecast(days=1)
        
        assert len(forecasts) >= 24  # At least 24 hours


# =============================================================================
# Run directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
