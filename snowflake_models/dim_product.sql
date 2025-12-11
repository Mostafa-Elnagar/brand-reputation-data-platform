-- Dimension Table: Product (Gold Layer/ MART)
-- Source: smartphones_info (Silver)
-- Schema: MART
USE SCHEMA MART;

CREATE OR REPLACE TABLE dim_product (
    product_key INT IDENTITY (1,1) PRIMARY KEY,
    brand_name VARCHAR,
    model VARCHAR,
    price_usd FLOAT,
    avg_rating FLOAT,
    is_5g BOOLEAN,
    processor_brand VARCHAR,
    num_cores INT,
    processor_speed FLOAT,
    battery_capacity INT,
    fast_charging_available BOOLEAN,
    fast_charging INT,
    ram_capacity INT,
    internal_memory INT,
    screen_size FLOAT,
    refresh_rate INT,
    num_rear_cameras INT,
    os VARCHAR,
    primary_camera_rear INT,
    primary_camera_front INT,
    extended_memory_available BOOLEAN,
    resolution_height INT,
    resolution_width INT
);

INSERT INTO dim_product (
    brand_name,
    model,
    price_usd,
    avg_rating,
    is_5g,
    processor_brand,
    num_cores,
    processor_speed,
    battery_capacity,
    fast_charging_available,
    fast_charging,
    ram_capacity,
    internal_memory,
    screen_size,
    refresh_rate,
    num_rear_cameras,
    os,
    primary_camera_rear,
    primary_camera_front,
    extended_memory_available,
    resolution_height,
    resolution_width
)
    SELECT
        brand_name,
        model,
        price * 0.011,
        avg_rating,
        CASE WHEN "5G_OR_NOT" = 1 THEN TRUE ELSE FALSE END,
        processor_brand,
        num_cores,
        processor_speed,
        battery_capacity,
        CASE WHEN fast_charging_available = 1 THEN TRUE ELSE FALSE END,
        fast_charging,
        ram_capacity,
        internal_memory,
        screen_size,
        refresh_rate,
        num_rear_cameras,
        os,
        primary_camera_rear,
        primary_camera_front,
        CASE WHEN extended_memory_available = 1 THEN TRUE ELSE FALSE END,
        resolution_height,
        resolution_width
    FROM iceberg_tables.smartphones_info;