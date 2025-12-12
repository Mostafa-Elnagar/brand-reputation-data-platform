-- Fact Table: Brand Mentions (Gold Layer)
-- Joins: sentiment (base), comments & submissions (context), dim_product (FK)
-- Schema: MART
USE SCHEMA MART;

CREATE OR REPLACE TABLE fact_brand_mentions AS
SELECT
    dp.product_key,
    ds.submission_key,
    dd.date_key,
    s.comment_id,
    s.brand_name,
    s.product_name,
    s.aspect,
    s.sentiment_score,
    c.score AS comment_score
FROM ICEBERG_TABLES.reddit_sentiment s
LEFT JOIN dim_product dp
    ON lower(s.brand_name) = lower(dp.brand_name) 
    AND lower(s.product_name) = lower(dp.model)
LEFT JOIN ICEBERG_TABLES.reddit_comments c 
    ON s.comment_id = c.id
LEFT JOIN dim_submission ds
    -- Prio 1: Join via Comment's parent link (Strip 't3_' prefix to match short ID)
    ON SPLIT_PART(c.link_id, '_', 2) = ds.reddit_submission_id
    -- Prio 2: Fallback to Sentiment's submission_id (Direct Match)
    OR s.submission_id = ds.reddit_submission_id
LEFT JOIN dim_date dd
    ON TO_DATE(to_timestamp(c.created_utc)) = dd.full_date;