"""
Fetcher DAG - Weather Data Ingestion Pipeline

This DAG fetches current weather data from OpenWeatherMap API for multiple cities
and stores both raw and parsed data in Postgres.

Tasks:
    1. create_tables: Ensures all required tables exist
    2. fetch_and_store_weather: Fetches from API and stores to DB

Prerequisites:
    - Set Airflow Variable: OPENWEATHER_API_KEY
    - Postgres connection: postgres_default
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from airflow.providers.postgres.operators.postgres import PostgresOperator

# Add src to path
sys.path.insert(0, "/opt/airflow/src")

from config import (
    LOCATIONS,
    POSTGRES_CONN_ID,
    AIRFLOW_VAR_API_KEY,
)
from extract import WeatherDataExtractor
from queries import (
    get_postgres_hook,
    get_or_create_location,
    store_raw_weather,
    store_parsed_weather,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_store_weather(**context):
    """
    Fetch weather data and store directly to database.
    
    Uses:
        - WeatherDataExtractor (from extract.py) for API calls
        - Database functions (from utils.py) for storage
    
    Flow for each location:
        1. Get/create location_id
        2. Extract weather from API
        3. Store raw JSON
        4. Parse and store measurements
    """
    
    # Get API key from Airflow Variables
    api_key = Variable.get(AIRFLOW_VAR_API_KEY)
    logger.info(f"API key loaded: {api_key[:5]}...")
    
    # Initialize extractor and database hook
    extractor = WeatherDataExtractor(api_key=api_key)
    hook = get_postgres_hook()
    
    # Track results
    success_count = 0
    error_count = 0
    errors = []
    
    # Process each location
    for location, result in extractor.extract_all():
        city_name = location["city_name"]
        
        try:
            logger.info(f"Processing: {city_name}")
            
            # Check if extraction succeeded
            if result is None:
                raise ValueError("API extraction failed")
            
            api_response = result["api_response"]
            observed_at = result["observed_at"]
            
            # Step 1: Get or create location
            location_id = get_or_create_location(
                hook=hook,
                city_name=city_name,
                country_code=location["country_code"],
                lat=location["lat"],
                lon=location["lon"],
                timezone_offset=api_response.get("timezone"),
            )
            
            # Step 2: Store raw data
            raw_id = store_raw_weather(
                hook=hook,
                location_id=location_id,
                api_response=api_response,
                observed_at=observed_at,
            )
            
            # Step 3: Parse and store measurements
            parsed_data = extractor.parse_response(api_response, location_id)
            parsed_id = store_parsed_weather(hook=hook, parsed_data=parsed_data)
            
            # Log success
            temp = parsed_data.get("temperature")
            weather = parsed_data.get("weather_main")
            logger.info(f"  SUCCESS: {city_name} -> {temp}°C, {weather}")
            logger.info(f"  IDs: location={location_id}, raw={raw_id}, parsed={parsed_id}")
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"  ERROR: {city_name} -> {e}")
            errors.append({"city": city_name, "error": str(e)})
            error_count += 1
    
    # Summary
    logger.info(f"PIPELINE COMPLETE: {success_count} success, {error_count} errors")
    if errors:
        for err in errors:
            logger.warning(f"  Failed: {err['city']} - {err['error']}")
    
    return {
        "success_count": success_count,
        "error_count": error_count,
        "errors": errors,
    }

with DAG(
    dag_id="fetcher",
    default_args=default_args,
    description="Fetch weather data from OpenWeatherMap API and store in Postgres",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "ingestion", "take-home"],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Create tables
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/schema.sql",
    )
    
    # Task 2: Fetch and store weather
    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_weather",
        python_callable=fetch_and_store_weather,
        provide_context=True,
    )
    
    # Dependencies
    create_tables >> fetch_and_store