"""
Reddit Bronze Layer Ingestion Glue Job.

This script reads raw JSONL data from the Landing Zone and ingests it into
Bronze layer Iceberg tables with deduplication using MERGE INTO.

Usage:
    Triggered via AWS Glue Job with the following parameters:
    --LANDING_BUCKET: S3 bucket containing raw JSONL files
    --DATABASE_NAME: Glue catalog database name for Bronze tables
    --PROCESS_DATE: Optional date to process (DD-MM-YYYY format), defaults to today
"""

import sys
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, input_file_name, current_timestamp,
    regexp_extract, when, to_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, MapType, TimestampType
)


def get_submissions_schema():
    """Define strict schema for Reddit submissions."""
    return StructType([
        StructField("id", StringType(), True),
        StructField("fullname_id", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("author", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("created_utc", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("score", IntegerType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("num_comments", IntegerType(), True),
        StructField("is_self", BooleanType(), True),
        StructField("stickied", BooleanType(), True),
        StructField("locked", BooleanType(), True),
        StructField("distinguished", StringType(), True),
        StructField("permalink", StringType(), True),
        StructField("url", StringType(), True),
        StructField("edited", LongType(), True),
        StructField("is_video", BooleanType(), True),
        StructField("media_only", BooleanType(), True),
        StructField("thumbnail", StringType(), True),
        StructField("awards", MapType(StringType(), IntegerType()), True),
    ])


def get_comments_schema():
    """Define strict schema for Reddit comments."""
    return StructType([
        StructField("id", StringType(), True),
        StructField("fullname_id", StringType(), True),
        StructField("submission_id", StringType(), True),
        StructField("link_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("author", StringType(), True),
        StructField("body", StringType(), True),
        StructField("created_utc", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("score", IntegerType(), True),
        StructField("controversiality", IntegerType(), True),
        StructField("is_submitter", BooleanType(), True),
        StructField("distinguished", StringType(), True),
        StructField("edited", LongType(), True),
        StructField("permalink", StringType(), True),
        StructField("depth", IntegerType(), True),
        StructField("awards", MapType(StringType(), IntegerType()), True),
    ])


def add_audit_columns(df):
    """Add audit columns for data lineage."""
    return df.withColumn(
        "ingestion_timestamp", current_timestamp()
    ).withColumn(
        "source_file", input_file_name()
    ).withColumn(
        "source_sort_type",
        regexp_extract(input_file_name(), r"/reddit/(\w+)/", 1)
    ).withColumn(
        "partition_date",
        regexp_extract(input_file_name(), r"/date=([^/]+)/", 1)
    )


def process_submissions(spark, glue_context, landing_path, database_name, table_name):
    """
    Read submissions from Landing Zone and merge into Bronze Iceberg table.
    
    S3 path structure: reddit/{sort_type}/subreddit={name}/date={DD-MM-YYYY}/submissions.jsonl
    """
    print(f"Processing submissions from: {landing_path}")
    
    schema = get_submissions_schema()
    
    # Use explicit glob pattern matching S3 structure
    submissions_path = f"{landing_path}/*/subreddit=*/date=*/submissions.jsonl"
    print(f"Reading from: {submissions_path}")
    
    df = spark.read.schema(schema).json(submissions_path)
    
    if df.rdd.isEmpty():
        print("No submission files found. Skipping.")
        return 0
    
    df = add_audit_columns(df)
    df = df.drop("awards")
    
    record_count = df.count()
    print(f"Found {record_count} submission records to process")
    
    df.createOrReplaceTempView("submissions_staging")
    
    qualified_table = f"glue_catalog.{database_name}.{table_name}"
    
    merge_sql = f"""
        MERGE INTO {qualified_table} AS target
        USING submissions_staging AS source
        ON target.id = source.id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    try:
        spark.sql(merge_sql)
        print(f"Successfully merged {record_count} submissions into {table_name}")
    except Exception as e:
        print(f"MERGE failed, attempting INSERT: {e}")
        df.writeTo(qualified_table).append()
        print(f"Appended {record_count} submissions to {table_name}")
    
    return record_count


def process_comments(spark, glue_context, landing_path, database_name, table_name):
    """
    Read comments from Landing Zone and merge into Bronze Iceberg table.
    
    S3 path structure: reddit/{sort_type}/subreddit={name}/date={DD-MM-YYYY}/comments.jsonl
    """
    print(f"Processing comments from: {landing_path}")
    
    schema = get_comments_schema()
    
    # Use explicit glob pattern matching S3 structure
    comments_path = f"{landing_path}/*/subreddit=*/date=*/comments.jsonl"
    print(f"Reading from: {comments_path}")
    
    df = spark.read.schema(schema).json(comments_path)
    
    if df.rdd.isEmpty():
        print("No comment files found. Skipping.")
        return 0
    
    df = add_audit_columns(df)
    df = df.drop("awards")
    
    record_count = df.count()
    print(f"Found {record_count} comment records to process")
    
    df.createOrReplaceTempView("comments_staging")
    
    qualified_table = f"glue_catalog.{database_name}.{table_name}"
    
    merge_sql = f"""
        MERGE INTO {qualified_table} AS target
        USING comments_staging AS source
        ON target.id = source.id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    try:
        spark.sql(merge_sql)
        print(f"Successfully merged {record_count} comments into {table_name}")
    except Exception as e:
        print(f"MERGE failed, attempting INSERT: {e}")
        df.writeTo(qualified_table).append()
        print(f"Appended {record_count} comments to {table_name}")
    
    return record_count


def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'LANDING_BUCKET',
        'DATABASE_NAME',
    ])
    
    job_name = args['JOB_NAME']
    landing_bucket = args['LANDING_BUCKET']
    database_name = args['DATABASE_NAME']
    
    process_date = args.get('PROCESS_DATE')
    if not process_date:
        now = datetime.now(timezone.utc)
        process_date = f"{now.day:02d}-{now.month:02d}-{now.year}"
    
    landing_path = f"s3://{landing_bucket}/reddit"
    
    print(f"Starting Bronze ingestion job: {job_name}")
    print(f"Landing path: {landing_path}")
    print(f"Database: {database_name}")
    print(f"Process date: {process_date}")
    
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{landing_bucket.replace('-lz', '-augmented')}/bronze/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(job_name, args)
    
    submissions_count = process_submissions(
        spark, glue_context, landing_path,
        database_name, "submissions"
    )
    
    comments_count = process_comments(
        spark, glue_context, landing_path,
        database_name, "comments"
    )
    
    print(f"Job completed. Submissions: {submissions_count}, Comments: {comments_count}")
    
    job.commit()


if __name__ == "__main__":
    main()

