-- ============================================================================
-- STRYKER DATA ENGINEERING EXERCISE - WEATHER DATA PIPELINE SCHEMA
-- ============================================================================
-- I'd like to propose a Medallion Architecture (Bronze/Silver/Gold) schema
-- for storing weather data ingested from the OpenWeatherMap API.
-- Following a Schema Design: Bronze/Silver/Gold pattern (Medallion Architecture)
-- 
-- Tables:
--   1. dim_locations              - Dimension table for tracked cities
--   2. raw_weather_api_responses  - Bronze layer: raw JSON from API
--   3. fact_weather_measurements  - Silver layer: parsed, queryable data
--   4. agg_daily_weather          - Gold layer: pre-computed daily aggregates
--
-- Key Design Decisions:
--   - All tables have UNIQUE constraints for idempotency (safe re-runs)
--   - Raw JSON stored for reprocessing capability
--   - Timestamps stored in UTC for consistency
--   - Metric units used (Celsius, m/s, hPa)
--
-- ============================================================================
-- 1. DIMENSION TABLE: LOCATIONS
-- ============================================================================
-- Stores metadata for cities/coordinates we're tracking.
-- Normalized to avoid repeating city info in every measurement.
-- Population: Seeded from configuration, updated by fetcher if needed.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_locations (
    location_id         SERIAL PRIMARY KEY,
    city_name           VARCHAR(255) NOT NULL,
    country_code        VARCHAR(10),
    latitude            DECIMAL(9,6) NOT NULL,
    longitude           DECIMAL(9,6) NOT NULL,
    timezone_offset     INTEGER,                      
    
    -- Audit columns
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Natural key: one entry per coordinate pair
    CONSTRAINT uq_location_coords UNIQUE (latitude, longitude)
);

COMMENT ON TABLE dim_locations IS 'Dimension table storing metadata for tracked weather locations';
COMMENT ON COLUMN dim_locations.timezone_offset IS 'Timezone offset from UTC in seconds';


-- ============================================================================
-- 2. BRONZE LAYER: RAW API RESPONSES
-- ============================================================================
-- Immutable append-only storage of raw API responses.
-- 
-- Purpose:
--   - Source of truth for all downstream tables
--
-- Idempotency: UPSERT on (location_id, observed_at)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw_weather_api_responses (
    id                  SERIAL PRIMARY KEY,
    location_id         INTEGER NOT NULL REFERENCES dim_locations(location_id),
    
    -- Raw data
    api_response        JSONB NOT NULL,                 -- Complete API response
    
    -- Timestamps
    observed_at         TIMESTAMP NOT NULL,             -- 'dt' field from API (converted from Unix)
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Idempotency key: one record per location per observation time
    CONSTRAINT uq_raw_location_observed UNIQUE (location_id, observed_at)
);

COMMENT ON TABLE raw_weather_api_responses IS 'Bronze layer: raw JSON responses from OpenWeatherMap API';
COMMENT ON COLUMN raw_weather_api_responses.api_response IS 'Complete JSON response from API for reprocessing';
COMMENT ON COLUMN raw_weather_api_responses.observed_at IS 'Observation timestamp from API (dt field), stored in UTC';


-- ============================================================================
-- 3. SILVER LAYER: PARSED WEATHER MEASUREMENTS
-- ============================================================================
-- Parsed, validated, and queryable weather measurements.
-- 
-- This is the primary table for:
--   - Time-series analysis
--   - Ad-hoc queries
--   - Feature extraction (via window functions)
--
-- Units (metric):
--   - Temperature: Celsius
--   - Wind speed: meters/second
--   - Pressure: hPa
--   - Precipitation: mm
--   - Visibility: meters
--
-- Idempotency: UPSERT on (location_id, observed_at)
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_weather_measurements (
    id                      SERIAL PRIMARY KEY,
    location_id             INTEGER NOT NULL REFERENCES dim_locations(location_id),
    observed_at             TIMESTAMP NOT NULL,             -- 'dt' from API (UTC)
    
    -- Temperature measurements (Celsius)
    temperature             DECIMAL(5,2),                   -- Current temp
    feels_like              DECIMAL(5,2),                   -- Apparent temp
    temp_min                DECIMAL(5,2),                   -- Min temp in area
    temp_max                DECIMAL(5,2),                   -- Max temp in area
    
    -- Atmospheric measurements
    pressure                INTEGER,                        -- Sea level pressure (hPa)
    humidity                INTEGER,                        -- Relative humidity (%)
    sea_level_pressure      INTEGER,                        -- Sea level pressure (hPa) - may be NULL
    ground_level_pressure   INTEGER,                        -- Ground level pressure (hPa) - may be NULL
    
    -- Wind measurements (m/s)
    wind_speed              DECIMAL(6,2),                   -- Wind speed
    wind_deg                INTEGER,                        -- Wind direction (degrees)
    wind_gust               DECIMAL(6,2),                   -- Wind gust - may be NULL
    
    -- Visibility and clouds
    visibility              INTEGER,                        -- Visibility (meters), max 10000
    cloudiness              INTEGER,                        -- Cloud cover (%)
    
    -- Precipitation (mm/h) - NULL when not occurring
    rain_1h                 DECIMAL(6,2),                   -- Rain volume last hour
    snow_1h                 DECIMAL(6,2),                   -- Snow volume last hour
    
    -- Weather condition (primary condition from array)
    weather_id              INTEGER,                        -- Condition code (e.g., 800 = clear)
    weather_main            VARCHAR(50),                    -- Group: "Rain", "Clouds", "Clear", etc.
    weather_description     VARCHAR(255),                   -- Detail: "light rain", "overcast clouds"
    weather_icon            VARCHAR(10),                    -- Icon code for UI
    
    -- Sun times (UTC)
    sunrise                 TIMESTAMP,
    sunset                  TIMESTAMP,
    
    -- Audit
    fetched_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Idempotency key
    CONSTRAINT uq_measurement_location_observed UNIQUE (location_id, observed_at)
);

COMMENT ON TABLE fact_weather_measurements IS 'Silver layer: parsed weather measurements for time-series analysis';
COMMENT ON COLUMN fact_weather_measurements.weather_id IS 'OpenWeatherMap condition code - see https://openweathermap.org/weather-conditions';
COMMENT ON COLUMN fact_weather_measurements.rain_1h IS 'Precipitation in last hour (mm). NULL if no rain.';
COMMENT ON COLUMN fact_weather_measurements.snow_1h IS 'Snowfall in last hour (mm). NULL if no snow.';


-- ============================================================================
-- 4. GOLD LAYER: DAILY AGGREGATES
-- ============================================================================
-- Pre-computed daily statistics for dashboards and reporting.
--
-- Populated by: transformer DAG
-- Refresh strategy: Full recompute for affected dates (idempotent)
--
-- Use cases:
--   - Daily weather summaries
--   - Trend analysis (day-over-day)
--   - City comparisons
--   - Monthly/weekly rollups
-- ============================================================================

CREATE TABLE IF NOT EXISTS agg_daily_weather (
    id                      SERIAL PRIMARY KEY,
    location_id             INTEGER NOT NULL REFERENCES dim_locations(location_id),
    date                    DATE NOT NULL,
    
    -- Temperature aggregates (Celsius)
    temp_min                DECIMAL(5,2),                   -- Minimum of the day
    temp_max                DECIMAL(5,2),                   -- Maximum of the day
    temp_avg                DECIMAL(5,2),                   -- Average of the day
    
    -- Other measurement averages
    humidity_avg            DECIMAL(5,2),                   -- Avg humidity (%)
    pressure_avg            DECIMAL(7,2),                   -- Avg pressure (hPa)
    wind_speed_avg          DECIMAL(6,2),                   -- Avg wind speed (m/s)
    wind_speed_max          DECIMAL(6,2),                   -- Max wind speed (m/s)
    cloudiness_avg          DECIMAL(5,2),                   -- Avg cloud cover (%)
    
    -- Precipitation totals (mm)
    total_rain_mm           DECIMAL(8,2) DEFAULT 0,         -- Sum of rain_1h
    total_snow_mm           DECIMAL(8,2) DEFAULT 0,         -- Sum of snow_1h
    
    -- Data quality
    observation_count       INTEGER,                        -- Number of observations
    
    -- Dominant condition (most frequent weather_main)
    dominant_weather        VARCHAR(50),
    
    -- Audit
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Idempotency key
    CONSTRAINT uq_daily_location_date UNIQUE (location_id, date)
);

COMMENT ON TABLE agg_daily_weather IS 'Gold layer: pre-computed daily weather aggregates for reporting';
COMMENT ON COLUMN agg_daily_weather.observation_count IS 'Number of measurements used in aggregation - useful for data quality';
COMMENT ON COLUMN agg_daily_weather.dominant_weather IS 'Most frequently observed weather_main for the day';


-- ============================================================================
-- 5. INDEXES FOR QUERY PERFORMANCE
-- ============================================================================
-- Optimized for common access patterns:
--   - Time-series queries (location + time range)
--   - Recent data lookups (time descending)
--   - Daily aggregation queries
-- ============================================================================

-- Fact table: primary access pattern is location + time range
CREATE INDEX IF NOT EXISTS idx_measurements_location_time 
    ON fact_weather_measurements(location_id, observed_at DESC);

-- Fact table: queries filtering by time only
CREATE INDEX IF NOT EXISTS idx_measurements_observed_at 
    ON fact_weather_measurements(observed_at DESC);

-- Raw table: for reprocessing queries
CREATE INDEX IF NOT EXISTS idx_raw_location_observed 
    ON raw_weather_api_responses(location_id, observed_at DESC);

-- Daily aggregates: primary access pattern
CREATE INDEX IF NOT EXISTS idx_daily_location_date 
    ON agg_daily_weather(location_id, date DESC);

-- Daily aggregates: date-only queries (cross-city comparisons)
CREATE INDEX IF NOT EXISTS idx_daily_date 
    ON agg_daily_weather(date DESC);

-- Weekly aggregates: primary access pattern
CREATE INDEX IF NOT EXISTS idx_weekly_location_week
    ON agg_weekly_weather(location_id, year_week DESC);

-- Weekly aggregates: week-only queries
CREATE INDEX IF NOT EXISTS idx_weekly_week
    ON agg_weekly_weather(year_week DESC);

-- Location summary: by location (most common query)
CREATE INDEX IF NOT EXISTS idx_location_summary_location
    ON agg_location_summary(location_id);


-- ============================================================================
-- 5. GOLD LAYER: WEEKLY AGGREGATES
-- ============================================================================
-- Pre-computed weekly statistics for trend analysis.
--
-- Populated by: transformer DAG
-- Useful for: Week-over-week comparisons, smoothing daily noise
-- ============================================================================

CREATE TABLE IF NOT EXISTS agg_weekly_weather (
    id                      SERIAL PRIMARY KEY,
    location_id             INTEGER NOT NULL REFERENCES dim_locations(location_id),
    year_week               VARCHAR(10) NOT NULL,                  -- Format: YYYY-Www (ISO week)
    
    -- Temperature aggregates (Celsius)
    temp_min                DECIMAL(5,2),                          -- Minimum of the week
    temp_max                DECIMAL(5,2),                          -- Maximum of the week
    temp_avg                DECIMAL(5,2),                          -- Average of the week
    
    -- Other measurement averages
    humidity_avg            DECIMAL(5,2),                          -- Avg humidity (%)
    pressure_avg            DECIMAL(7,2),                          -- Avg pressure (hPa)
    wind_speed_avg          DECIMAL(6,2),                          -- Avg wind speed (m/s)
    wind_speed_max          DECIMAL(6,2),                          -- Max wind speed (m/s)
    cloudiness_avg          DECIMAL(5,2),                          -- Avg cloud cover (%)
    
    -- Precipitation totals (mm)
    total_rain_mm           DECIMAL(8,2) DEFAULT 0,                -- Sum of rain
    total_snow_mm           DECIMAL(8,2) DEFAULT 0,                -- Sum of snow
    
    -- Data quality
    observation_count       INTEGER,                               -- Number of observations in week
    
    -- Audit
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Idempotency key
    CONSTRAINT uq_weekly_location_week UNIQUE (location_id, year_week)
);

COMMENT ON TABLE agg_weekly_weather IS 'Gold layer: pre-computed weekly weather aggregates for trend analysis';
COMMENT ON COLUMN agg_weekly_weather.year_week IS 'ISO week format: YYYY-Www (e.g., 2026-W03)';


-- ============================================================================
-- 6. GOLD LAYER: LOCATION SUMMARY
-- ============================================================================
-- All-time statistics per location for historical analysis and location comparisons.
--
-- Populated by: transformer DAG (full recompute)
-- Useful for: Location comparisons, baseline analytics, extremes identification
-- ============================================================================

CREATE TABLE IF NOT EXISTS agg_location_summary (
    id                      SERIAL PRIMARY KEY,
    location_id             INTEGER NOT NULL UNIQUE REFERENCES dim_locations(location_id),
    
    -- Temperature statistics (Celsius)
    temp_min_all_time       DECIMAL(5,2),                          -- Absolute minimum
    temp_max_all_time       DECIMAL(5,2),                          -- Absolute maximum
    temp_avg_all_time       DECIMAL(5,2),                          -- Average across all observations
    
    -- Atmospheric statistics
    humidity_avg_all_time   DECIMAL(5,2),                          -- Average humidity (%)
    pressure_avg_all_time   DECIMAL(7,2),                          -- Average pressure (hPa)
    wind_speed_max_all_time DECIMAL(6,2),                          -- Peak wind speed (m/s)
    
    -- Precipitation statistics (mm)
    total_rain_all_time     DECIMAL(10,2) DEFAULT 0,               -- Total rain observed
    total_snow_all_time     DECIMAL(10,2) DEFAULT 0,               -- Total snow observed
    rain_days_count         INTEGER DEFAULT 0,                     -- Days with rain
    snow_days_count         INTEGER DEFAULT 0,                     -- Days with snow
    
    -- Data coverage
    first_observation_date  DATE,                                  -- Earliest data point
    last_observation_date   DATE,                                  -- Latest data point
    total_observations      INTEGER,                               -- Total records for this location
    total_days_covered      INTEGER,                               -- Unique days with data
    
    -- Audit
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE agg_location_summary IS 'Gold layer: all-time statistics per location for historical analysis and comparisons';
COMMENT ON COLUMN agg_location_summary.total_days_covered IS 'Count of unique dates with observations (useful for data completeness)';


-- ============================================================================
-- 7. SEED DATA: INITIAL LOCATIONS
-- ============================================================================
-- Pre-populate with a diverse set of cities for testing.
-- Using ON CONFLICT to make this idempotent.
-- ============================================================================

INSERT INTO dim_locations (city_name, country_code, latitude, longitude)
VALUES 
    ('London', 'GB', 51.5074, -0.1278),
    ('Paris', 'FR', 48.8566, 2.3522),
    ('New York', 'US', 40.7128, -74.0060),
    ('Tokyo', 'JP', 35.6762, 139.6503),
    ('Sydney', 'AU', -33.8688, 151.2093),
    ('São Paulo', 'BR', -23.5505, -46.6333),
    ('Mumbai', 'IN', 19.0760, 72.8777),
    ('Cairo', 'EG', 30.0444, 31.2357)
ON CONFLICT (latitude, longitude) DO UPDATE SET
    city_name = EXCLUDED.city_name,
    country_code = EXCLUDED.country_code,
    updated_at = CURRENT_TIMESTAMP;


-- ============================================================================
-- VERIFICATION QUERIES (for testing)
-- ============================================================================
-- Uncomment to verify schema creation:
--
-- SELECT * FROM dim_locations;
-- SELECT column_name, data_type, is_nullable 
--   FROM information_schema.columns 
--   WHERE table_name = 'fact_weather_measurements';
-- ============================================================================