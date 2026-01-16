"""
Database Utilities Module.

This module handles all database operations:
    - Location management (dim_locations)
    - Raw data storage (raw_weather_api_responses)
    - Parsed data storage (fact_weather_measurements)

All functions use PostgresHook for Airflow integration.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import POSTGRES_CONN_ID

logger = logging.getLogger(__name__)


def get_postgres_hook(conn_id: str = POSTGRES_CONN_ID) -> PostgresHook:
    """Get PostgresHook instance."""
    return PostgresHook(postgres_conn_id=conn_id)


def get_or_create_location(
    hook: PostgresHook,
    city_name: str,
    country_code: str,
    lat: float,
    lon: float,
    timezone_offset: Optional[int] = None
) -> int:
    """
    Get or create a location in dim_locations table.
    
    Uses UPSERT for idempotency.
    
    """
    sql = """
        INSERT INTO dim_locations (city_name, country_code, latitude, longitude, timezone_offset)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude) DO UPDATE SET
            city_name = EXCLUDED.city_name,
            country_code = EXCLUDED.country_code,
            timezone_offset = COALESCE(EXCLUDED.timezone_offset, dim_locations.timezone_offset),
            updated_at = CURRENT_TIMESTAMP
        RETURNING location_id;
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql, (city_name, country_code, lat, lon, timezone_offset))
        location_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Location '{city_name}' -> location_id={location_id}")
        return location_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error upserting location '{city_name}': {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def store_raw_weather(
    hook: PostgresHook,
    location_id: int,
    api_response: Dict[str, Any],
    observed_at: datetime
) -> int:
    """
    Store raw API response in raw_weather_api_responses table.
    
    Uses UPSERT for idempotency.
    """
    sql = """
        INSERT INTO raw_weather_api_responses (location_id, api_response, observed_at, fetched_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (location_id, observed_at) DO UPDATE SET
            api_response = EXCLUDED.api_response,
            fetched_at = CURRENT_TIMESTAMP
        RETURNING id;
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql, (location_id, json.dumps(api_response), observed_at))
        record_id = cursor.fetchone()[0]
        conn.commit()
        logger.debug(f"Raw data stored: location_id={location_id}, id={record_id}")
        return record_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error storing raw data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def store_parsed_weather(hook: PostgresHook, parsed_data: Dict[str, Any]) -> int:
    """
    Store parsed weather data in fact_weather_measurements table.
    
    Uses UPSERT for idempotency.

    """
    sql = """
        INSERT INTO fact_weather_measurements (
            location_id, observed_at,
            temperature, feels_like, temp_min, temp_max,
            pressure, humidity, sea_level_pressure, ground_level_pressure,
            wind_speed, wind_deg, wind_gust,
            visibility, cloudiness,
            rain_1h, snow_1h,
            weather_id, weather_main, weather_description, weather_icon,
            sunrise, sunset,
            fetched_at
        )
        VALUES (
            %(location_id)s, %(observed_at)s,
            %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s,
            %(pressure)s, %(humidity)s, %(sea_level_pressure)s, %(ground_level_pressure)s,
            %(wind_speed)s, %(wind_deg)s, %(wind_gust)s,
            %(visibility)s, %(cloudiness)s,
            %(rain_1h)s, %(snow_1h)s,
            %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s,
            %(sunrise)s, %(sunset)s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (location_id, observed_at) DO UPDATE SET
            temperature = EXCLUDED.temperature,
            feels_like = EXCLUDED.feels_like,
            temp_min = EXCLUDED.temp_min,
            temp_max = EXCLUDED.temp_max,
            pressure = EXCLUDED.pressure,
            humidity = EXCLUDED.humidity,
            sea_level_pressure = EXCLUDED.sea_level_pressure,
            ground_level_pressure = EXCLUDED.ground_level_pressure,
            wind_speed = EXCLUDED.wind_speed,
            wind_deg = EXCLUDED.wind_deg,
            wind_gust = EXCLUDED.wind_gust,
            visibility = EXCLUDED.visibility,
            cloudiness = EXCLUDED.cloudiness,
            rain_1h = EXCLUDED.rain_1h,
            snow_1h = EXCLUDED.snow_1h,
            weather_id = EXCLUDED.weather_id,
            weather_main = EXCLUDED.weather_main,
            weather_description = EXCLUDED.weather_description,
            weather_icon = EXCLUDED.weather_icon,
            sunrise = EXCLUDED.sunrise,
            sunset = EXCLUDED.sunset,
            fetched_at = CURRENT_TIMESTAMP
        RETURNING id;
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql, parsed_data)
        record_id = cursor.fetchone()[0]
        conn.commit()
        logger.debug(f"Parsed data stored: location_id={parsed_data['location_id']}, id={record_id}")
        return record_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error storing parsed data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def compute_daily_aggregates(hook: PostgresHook, sql: str) -> int:
    """
    Compute and store daily weather aggregates.
    
    Aggregates fact_weather_measurements into agg_daily_weather.
    Uses UPSERT for idempotency.
    
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
        logger.info(f"Daily aggregates: {rows_affected} rows upserted")
        return rows_affected
    except Exception as e:
        conn.rollback()
        logger.error(f"Error computing daily aggregates: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def validate_daily_aggregates(hook: PostgresHook, sql: str) -> Dict[str, Any]:
    """
    Validate quality of daily aggregates.
    
    Checks for:
        - NULL critical fields
        - Values outside reasonable ranges
        - Insufficient observation counts

    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        issues = cursor.fetchall()
        
        if issues:
            logger.warning(f"Found {len(issues)} data quality issues")
            for issue in issues:
                logger.warning(
                    f"  location_id={issue[0]}, date={issue[1]}, "
                    f"count={issue[2]}, temp_avg={issue[3]}"
                )
        else:
            logger.info("All validation checks passed ✓")
        
        return {
            "validation_issues": len(issues),
            "issues": issues,
        }
    except Exception as e:
        logger.error(f"Error validating aggregates: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def compute_weekly_aggregates(hook: PostgresHook, sql: str) -> int:
    """
    Compute and store weekly weather aggregates.
    
    Aggregates daily data into weekly summaries.
    Uses UPSERT for idempotency.
    
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
        logger.info(f"Weekly aggregates: {rows_affected} rows upserted")
        return rows_affected
    except Exception as e:
        conn.rollback()
        logger.error(f"Error computing weekly aggregates: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def compute_location_summary(hook: PostgresHook, sql: str) -> int:
    """
    Compute and store location summary (all-time statistics).
    
    Creates comprehensive statistics per location across all observations.
    Uses UPSERT for idempotency.

    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
        logger.info(f"Location summary: {rows_affected} rows upserted")
        return rows_affected
    except Exception as e:
        conn.rollback()
        logger.error(f"Error computing location summary: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def get_aggregation_sql(days_lookback: int = 7) -> str:
    """
    Get SQL query for computing daily aggregates.
        
    Returns:
        SQL INSERT...ON CONFLICT statement
    """
    sql = f"""
        INSERT INTO agg_daily_weather (
            location_id, date,
            temp_min, temp_max, temp_avg,
            humidity_avg, pressure_avg, wind_speed_avg, wind_speed_max, cloudiness_avg,
            total_rain_mm, total_snow_mm,
            observation_count, dominant_weather,
            created_at, updated_at
        )
        WITH daily_data AS (
            -- Compute all aggregates for each location+date
            SELECT
                location_id,
                DATE(observed_at) AS date,
                MIN(temperature) AS temp_min,
                MAX(temperature) AS temp_max,
                ROUND(AVG(temperature)::numeric, 2) AS temp_avg,
                ROUND(AVG(humidity)::numeric, 2) AS humidity_avg,
                ROUND(AVG(pressure)::numeric, 2) AS pressure_avg,
                ROUND(AVG(wind_speed)::numeric, 2) AS wind_speed_avg,
                MAX(wind_speed) AS wind_speed_max,
                ROUND(AVG(cloudiness)::numeric, 2) AS cloudiness_avg,
                COALESCE(ROUND(SUM(rain_1h)::numeric, 2), 0) AS total_rain_mm,
                COALESCE(ROUND(SUM(snow_1h)::numeric, 2), 0) AS total_snow_mm,
                COUNT(*) AS observation_count
            FROM fact_weather_measurements
            WHERE DATE(observed_at) >= DATE(CURRENT_TIMESTAMP) - INTERVAL '{days_lookback} days'
            GROUP BY location_id, DATE(observed_at)
        ),
        dominant_weather_ranked AS (
            -- Find dominant weather for each location+date using window functions
            SELECT
                location_id,
                DATE(observed_at) AS date,
                weather_main,
                ROW_NUMBER() OVER (
                    PARTITION BY location_id, DATE(observed_at)
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM fact_weather_measurements
            WHERE DATE(observed_at) >= DATE(CURRENT_TIMESTAMP) - INTERVAL '{days_lookback} days'
              AND weather_main IS NOT NULL
            GROUP BY location_id, DATE(observed_at), weather_main
        )
        SELECT
            dd.location_id,
            dd.date,
            dd.temp_min,
            dd.temp_max,
            dd.temp_avg,
            dd.humidity_avg,
            dd.pressure_avg,
            dd.wind_speed_avg,
            dd.wind_speed_max,
            dd.cloudiness_avg,
            dd.total_rain_mm,
            dd.total_snow_mm,
            dd.observation_count,
            dwr.weather_main AS dominant_weather,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM daily_data dd
        LEFT JOIN dominant_weather_ranked dwr
            ON dd.location_id = dwr.location_id
            AND dd.date = dwr.date
            AND dwr.rn = 1
        
        ON CONFLICT (location_id, date) DO UPDATE SET
            temp_min = EXCLUDED.temp_min,
            temp_max = EXCLUDED.temp_max,
            temp_avg = EXCLUDED.temp_avg,
            humidity_avg = EXCLUDED.humidity_avg,
            pressure_avg = EXCLUDED.pressure_avg,
            wind_speed_avg = EXCLUDED.wind_speed_avg,
            wind_speed_max = EXCLUDED.wind_speed_max,
            cloudiness_avg = EXCLUDED.cloudiness_avg,
            total_rain_mm = EXCLUDED.total_rain_mm,
            total_snow_mm = EXCLUDED.total_snow_mm,
            observation_count = EXCLUDED.observation_count,
            dominant_weather = EXCLUDED.dominant_weather,
            updated_at = CURRENT_TIMESTAMP
        ;
    """
    return sql