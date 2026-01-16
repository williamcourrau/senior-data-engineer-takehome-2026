"""
Weather Data Pipeline - Source Module

Modules:
    config.py   - Configuration and locations
    extract.py  - WeatherDataExtractor class (API calls)
    queries.py    - Database operations
"""

from sys import version


version = "1.0.0"

from .config import (
    LOCATIONS,
    OPENWEATHER_BASE_URL,
    OPENWEATHER_UNITS,
    AIRFLOW_VAR_API_KEY,
    POSTGRES_CONN_ID,
    API_CALL_DELAY,
    MAX_RETRIES_PER_LOCATION,
)

from .extract import WeatherDataExtractor

from .queries import (
    get_postgres_hook,
    get_or_create_location,
    store_raw_weather,
    store_parsed_weather,
)

__all__ = [
    # Config
    "LOCATIONS",
    "OPENWEATHER_BASE_URL",
    "OPENWEATHER_UNITS",
    "AIRFLOW_VAR_API_KEY",
    "POSTGRES_CONN_ID",
    "API_CALL_DELAY",
    "MAX_RETRIES_PER_LOCATION",
    "WeatherDataExtractor",
    "get_postgres_hook",
    "get_or_create_location",
    "store_raw_weather",
    "store_parsed_weather",
]