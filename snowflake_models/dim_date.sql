-- Dimension Table: Date (Gold Layer)
-- Generated Date Dimension
-- Schema: MART
USE SCHEMA MART;

CREATE OR REPLACE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(3),
    day_of_week INT,
    day_name VARCHAR(3),
    week_of_year INT,
    is_weekend BOOLEAN
);

INSERT INTO dim_date
WITH date_seq AS (
  SELECT DATEADD(day, SEQ4(), '2000-01-01')::DATE AS full_date
  FROM TABLE(GENERATOR(ROWCOUNT => 11500))
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INT AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    DAYOFWEEK(full_date) AS day_of_week,
    DAYNAME(full_date) AS day_name,
    WEEKOFYEAR(full_date) AS week_of_year,
    CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_seq;
