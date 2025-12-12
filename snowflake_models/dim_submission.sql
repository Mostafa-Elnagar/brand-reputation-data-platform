-- Dimension Table: Submission (Gold Layer/ MART)
-- Source: reddit_submissions (Silver)
-- Schema: MART
USE SCHEMA MART;

CREATE OR REPLACE TABLE dim_submission (
    submission_key INT IDENTITY(1,1) PRIMARY KEY,
    reddit_submission_id VARCHAR, -- Natural Key (Short ID, e.g. g6wj3z)
    subreddit VARCHAR,
    title VARCHAR,
    selftext VARCHAR,
    created_at TIMESTAMP_NTZ,
    num_comments INT,
    upvote_ratio FLOAT,
    score INT
);

INSERT INTO dim_submission (
    reddit_submission_id,
    subreddit,
    title,
    selftext,
    created_at,
    num_comments,
    upvote_ratio,
    score
)
SELECT 
    id,
    subreddit,
    title,
    selftext,
    to_timestamp(created_utc) AS created_at,
    num_comments,
    upvote_ratio,
    score
FROM iceberg_tables.reddit_submissions;
