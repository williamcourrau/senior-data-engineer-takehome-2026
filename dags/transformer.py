"""
Transformer DAG - Weather Data Transformation Pipeline

This DAG produces derived datasets from raw weather measurements for analytics and reporting.

Tasks:
    1. aggregate_daily_weather: Computes daily aggregates from fact_weather_measurements
    2. validate_aggregates: Quality checks on derived data

Use Cases:
    - Daily weather summaries for dashboards
    - Trend analysis (day-over-day changes)
    - Historical analysis and reporting
    - City-level comparisons

Prerequisites:
    - Fetcher DAG must run first to populate fact_weather_measurements
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Add src to path
sys.path.insert(0, "/opt/airflow/src")

from config import POSTGRES_CONN_ID
from queries import (
    get_postgres_hook,
    compute_daily_aggregates,
    get_aggregation_sql,
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

def aggregate_daily_weather(**context):
    """
    Orchestrate daily weather aggregation.
    
    Uses queries.get_aggregation_sql to generate SQL and
    queries.compute_daily_aggregates to execute it.
    """
    
    sql = get_aggregation_sql(days_lookback=7)
    
    hook = get_postgres_hook()
    rows_affected = compute_daily_aggregates(hook=hook, sql=sql)
    
    logger.info(f"Rows upserted: {rows_affected}")
    return {
        "status": "success",
        "rows_upserted": rows_affected,
    }

with DAG(
    dag_id="transformer",
    default_args=default_args,
    description="Transform raw weather data into derived datasets (daily, weekly aggregates and location summary) for analytics",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "transformation", "take-home"],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Ensure derived tables exist using schema file
    create_derived_tables = PostgresOperator(
        task_id="create_derived_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/schema.sql",
    )
    
    # Task 2: Compute daily aggregates from fact_weather_measurements and could be more aggregates in the future
    aggregate_daily = PythonOperator(
        task_id="aggregate_daily_weather",
        python_callable=aggregate_daily_weather,
        provide_context=True,
    )
    
    # Dependencies
    create_derived_tables >> aggregate_daily