"""
Configuration for the Weather Data Pipeline.

This module contains:
- List of locations to track
- API configuration constants
- Database table names

Note: API key is stored in Airflow Variables for security.
      Set it via Airflow UI: Admin > Variables > OPENWEATHER_API_KEY
"""


# Each location requires lat/lon for the API call.
# city_name and country_code are for our records (also returned by API).
LOCATIONS = [
    {"city_name": "London", "country_code": "GB", "lat": 51.5074, "lon": -0.1278},
    {"city_name": "Paris", "country_code": "FR", "lat": 48.8566, "lon": 2.3522},
    {"city_name": "New York", "country_code": "US", "lat": 40.7128, "lon": -74.0060},
    {"city_name": "Tokyo", "country_code": "JP", "lat": 35.6762, "lon": 139.6503},
]

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

OPENWEATHER_UNITS = "metric"

AIRFLOW_VAR_API_KEY = "OPENWEATHER_API_KEY"

POSTGRES_CONN_ID = "postgres_default"

TABLE_LOCATIONS = "dim_locations"
TABLE_RAW_WEATHER = "raw_weather_api_responses"
TABLE_MEASUREMENTS = "fact_weather_measurements"
TABLE_DAILY_AGG = "agg_daily_weather"

# Delay between API calls to avoid rate limiting (seconds)
API_CALL_DELAY = 1.0

# Maximum retries per location
MAX_RETRIES_PER_LOCATION = 2