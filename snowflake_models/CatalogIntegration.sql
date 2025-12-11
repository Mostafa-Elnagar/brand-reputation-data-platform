USE DATABASE BRAND_REP_DB;
USE SCHEMA ICEBERG_TABLES;

-- Create a Catalog Integration
-- Create an external volume
CREATE OR REPLACE EXTERNAL VOLUME external_vol
    STORAGE_LOCATIONS = (
        (
            NAME = 's3_data_location',
            STORAGE_PROVIDER = 'S3',
            STORAGE_BASE_URL = 's3://tf-prod-smartcomp-us-east-1-s3-lakehouse/brandrep_silver.db/',
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::912390896905:role/Snowflake_Iceberg_S3_Bucket_Access_Role'
        )
    );

DESCRIBE EXTERNAL VOLUME external_vol;

-- create glue catalog integration
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
    CATALOG_SOURCE = GLUE
    TABLE_FORMAT = ICEBERG
    CATALOG_NAMESPACE = 'brandrep_silver' 
    GLUE_AWS_ROLE_ARN = 'arn:aws:iam::912390896905:role/Snowflake_Iceberg_Glue_Catalog_Access_Role'
    GLUE_CATALOG_ID = '912390896905'
    GLUE_REGION = 'us-east-1'
    ENABLED = TRUE;

DESCRIBE CATALOG INTEGRATION glue_catalog_int;

-- Create the reference tables in Snowflake to access the Glue Catalog
-- Reference for brandrep_silver.comments_silver table
CREATE OR REPLACE ICEBERG TABLE reddit_comments
    EXTERNAL_VOLUME = 'external_vol'
    CATALOG = 'glue_catalog_int'
    CATALOG_TABLE_NAME = 'comments_silver'
    AUTO_REFRESH = TRUE;

-- Reference for brandrep_silver.submissions_silver table
CREATE OR REPLACE ICEBERG TABLE reddit_submissions
    EXTERNAL_VOLUME = 'external_vol'
    CATALOG = 'glue_catalog_int'
    CATALOG_TABLE_NAME = 'submissions_silver'
    AUTO_REFRESH = TRUE;

-- Reference for brandrep_silver.sentiment_analysis table
CREATE OR REPLACE ICEBERG TABLE reddit_sentiment
    EXTERNAL_VOLUME = 'external_vol'
    CATALOG = 'glue_catalog_int'
    CATALOG_TABLE_NAME = 'sentiment_analysis'
    AUTO_REFRESH = TRUE;

-- Reference for brandrep_silver.dim_products_ref table
CREATE OR REPLACE ICEBERG TABLE smartphones_info
    EXTERNAL_VOLUME = 'external_vol'
    CATALOG = 'glue_catalog_int'
    CATALOG_TABLE_NAME = 'dim_products_ref'
    AUTO_REFRESH = TRUE;

-- verifying the tables existence
SHOW TABLES;

-- Create a Schema for the gold layer (Data Mart)
CREATE SCHEMA IF NOT EXISTS MART;