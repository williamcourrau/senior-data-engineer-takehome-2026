"""
Weather Data Extraction Module.

This module handles:
    - Fetching data from OpenWeatherMap API
    - Validating API responses
    - Parsing responses into structured format
"""

import time
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

from config import (
    LOCATIONS,
    OPENWEATHER_BASE_URL,
    OPENWEATHER_UNITS,
    API_CALL_DELAY,
    MAX_RETRIES_PER_LOCATION,
)

logger = logging.getLogger(__name__)

class WeatherDataExtractor:
    """
    Extracts weather data from OpenWeatherMap API.
    
    This class handles API communication only.
    Database storage is handled separately by queries.py
    
    Example:
        extractor = WeatherDataExtractor(api_key="your_key")
        
        # Extract for one location
        result = extractor.extract_for_location(lat=51.5, lon=-0.12)
        
        # Extract for all configured locations
        for result in extractor.extract_all():
            print(result)
    """
    
    def __init__(
        self,
        api_key: str,
        api_url: str = OPENWEATHER_BASE_URL,
        units: str = OPENWEATHER_UNITS,
        locations: Optional[List[Dict]] = None,
        timeout: int = 30,
        retry_attempts: int = MAX_RETRIES_PER_LOCATION,
    ):
        """
        Initialize the extractor.
        """
        if not api_key:
            raise ValueError("Weather API key is required.")
        
        self.api_key = api_key
        self.api_url = api_url
        self.units = units
        self.locations = locations or LOCATIONS
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        
    
    def _make_api_request(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Make API request with retry logic.
        
        Returns:
            API response as dictionary, or empty dict on failure
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": self.units,
        }
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(
                    self.api_url,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{self.retry_attempts} failed: {e}")
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(API_CALL_DELAY)
                else:
                    logger.error(f"Failed after {self.retry_attempts} attempts")
                    return {}
        
        return {}
    
    def _validate_response(self, response: Dict[str, Any]) -> bool:
        """Validate API response has required fields."""
        if not response:
            return False
        
        if response.get("cod") != 200:
            logger.warning(f"Invalid response code: {response.get('cod')}")
            return False
        
        required = ["coord", "main", "dt", "weather"]
        for field in required:
            if field not in response:
                logger.warning(f"Missing field: {field}")
                return False
        
        return True
    
    def parse_response(self, api_response: Dict[str, Any], location_id: int) -> Dict[str, Any]:
        """
        Parse API response into structured format.
        
        Returns:
            Parsed data ready for database insertion
        """
        main = api_response.get("main", {})
        wind = api_response.get("wind", {})
        clouds = api_response.get("clouds", {})
        rain = api_response.get("rain", {})
        snow = api_response.get("snow", {})
        sys_data = api_response.get("sys", {})
        weather = api_response.get("weather", [{}])[0]
        
        observed_at = datetime.fromtimestamp(api_response["dt"], tz=timezone.utc)
        
        sunrise = None
        if sys_data.get("sunrise"):
            sunrise = datetime.fromtimestamp(sys_data["sunrise"], tz=timezone.utc)
        
        sunset = None
        if sys_data.get("sunset"):
            sunset = datetime.fromtimestamp(sys_data["sunset"], tz=timezone.utc)
        
        return {
            "location_id": location_id,
            "observed_at": observed_at,
            "temperature": main.get("temp"),
            "feels_like": main.get("feels_like"),
            "temp_min": main.get("temp_min"),
            "temp_max": main.get("temp_max"),
            "pressure": main.get("pressure"),
            "humidity": main.get("humidity"),
            "sea_level_pressure": main.get("sea_level"),
            "ground_level_pressure": main.get("grnd_level"),
            "wind_speed": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "wind_gust": wind.get("gust"),
            "visibility": api_response.get("visibility"),
            "cloudiness": clouds.get("all"),
            "rain_1h": rain.get("1h"),
            "snow_1h": snow.get("1h"),
            "weather_id": weather.get("id"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "weather_icon": weather.get("icon"),
            "sunrise": sunrise,
            "sunset": sunset,
        }
    
    def extract_for_location(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Extract weather data for a single location.
        
        Returns:
            Dict with api_response and observed_at, or None on failure
        """
        logger.info(f"Extracting weather for lat={lat}, lon={lon}")
        
        api_response = self._make_api_request(lat, lon)
        
        if not self._validate_response(api_response):
            return None
        
        observed_at = datetime.fromtimestamp(api_response["dt"], tz=timezone.utc)
        
        return {
            "api_response": api_response,
            "observed_at": observed_at,
        }
    
    def extract_all(self) -> List[Tuple[Dict, Dict]]:
        """
        Extract weather data for all configured locations.
        """
        for location in self.locations:
            result = self.extract_for_location(
                lat=location["lat"],
                lon=location["lon"],
            )
            
            yield location, result
            
            # Rate limiting
            time.sleep(API_CALL_DELAY)