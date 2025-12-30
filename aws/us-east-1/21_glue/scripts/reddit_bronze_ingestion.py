"""
Reddit Bronze Layer Ingestion Glue Job.

This script reads raw JSONL data from the Landing Zone and ingests it into
Bronze layer Iceberg tables with deduplication using MERGE INTO.

When triggered by the Step Function orchestrator, it processes only the freshly
ingested partition (filtered by PROCESS_DATE). When run standalone, it defaults
to today's UTC date.

Metrics are collected and written to:
- DynamoDB (per-subreddit metrics for each run)
- CloudWatch (aggregate metrics per run)

Usage:
    Triggered via AWS Glue Job with the following parameters:
    --LANDING_BUCKET: S3 bucket containing raw JSONL files
    --DATABASE_NAME: Glue catalog database name for Bronze tables
    --AUGMENTED_BUCKET: S3 bucket for Bronze Iceberg table warehouse
    --PROCESS_DATE: Optional date filter (DD-MM-YYYY format). When provided,
                    only processes data from partitions matching this date.
                    Defaults to today's UTC date if not specified.
    --METRICS_TABLE_NAME: DynamoDB table for storing Glue ingestion metrics.
    --ENVIRONMENT: Environment name (e.g., prod, dev) for metrics dimensions.
    --METRICS_NAMESPACE: CloudWatch namespace for metrics (default: RedditBronzeIngestion).
"""

import sys
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, input_file_name, current_timestamp,
    regexp_extract, when, to_timestamp, from_unixtime,
    min as spark_min, max as spark_max, count as spark_count,
    row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, MapType, TimestampType
)


# =============================================================================
# Metrics Data Classes
# =============================================================================

@dataclass
class SubredditMetrics:
    """Metrics for a single subreddit within a processing run."""
    subreddit: str
    submissions_count: int = 0
    comments_count: int = 0
    earliest_submission_created_utc: Optional[int] = None
    latest_submission_created_utc: Optional[int] = None
    earliest_comment_created_utc: Optional[int] = None
    latest_comment_created_utc: Optional[int] = None


@dataclass
class GlueRunMetrics:
    """Aggregate metrics for a Glue job run."""
    process_date: str
    run_id: str
    total_submissions: int = 0
    total_comments: int = 0
    subreddits_processed: int = 0
    earliest_submission_created_utc: Optional[int] = None
    latest_submission_created_utc: Optional[int] = None
    earliest_comment_created_utc: Optional[int] = None
    latest_comment_created_utc: Optional[int] = None
    subreddit_metrics: Dict[str, SubredditMetrics] = field(default_factory=dict)


# =============================================================================
# Metrics Writers
# =============================================================================

class GlueMetricsWriter:
    """Write Glue job metrics to DynamoDB and CloudWatch."""

    def __init__(
        self,
        dynamodb_table_name: str,
        cloudwatch_namespace: str,
        environment: str,
    ):
        self._dynamodb_table_name = dynamodb_table_name
        self._cloudwatch_namespace = cloudwatch_namespace
        self._environment = environment
        self._dynamodb = boto3.client("dynamodb")
        self._cloudwatch = boto3.client("cloudwatch")

    def write_metrics(self, metrics: GlueRunMetrics) -> None:
        """Write metrics to DynamoDB and CloudWatch."""
        self._write_to_dynamodb(metrics)
        self._publish_to_cloudwatch(metrics)

    def _write_to_dynamodb(self, metrics: GlueRunMetrics) -> None:
        """Write per-subreddit and aggregate metrics to DynamoDB."""
        ingested_at_ts = int(datetime.now(timezone.utc).timestamp())

        # Write aggregate run metrics
        aggregate_pk = f"ENV#{self._environment}#AGGREGATE"
        aggregate_sk = f"GLUE_RUN#{metrics.process_date}#{metrics.run_id}"

        aggregate_item = {
            "pk": {"S": aggregate_pk},
            "sk": {"S": aggregate_sk},
            "Environment": {"S": self._environment},
            "ProcessDate": {"S": metrics.process_date},
            "RunId": {"S": metrics.run_id},
            "TotalSubmissions": {"N": str(metrics.total_submissions)},
            "TotalComments": {"N": str(metrics.total_comments)},
            "SubredditsProcessed": {"N": str(metrics.subreddits_processed)},
            "IngestedAtTs": {"N": str(ingested_at_ts)},
        }

        if metrics.earliest_submission_created_utc is not None:
            aggregate_item["EarliestSubmissionCreatedUtc"] = {"N": str(metrics.earliest_submission_created_utc)}
        if metrics.latest_submission_created_utc is not None:
            aggregate_item["LatestSubmissionCreatedUtc"] = {"N": str(metrics.latest_submission_created_utc)}
        if metrics.earliest_comment_created_utc is not None:
            aggregate_item["EarliestCommentCreatedUtc"] = {"N": str(metrics.earliest_comment_created_utc)}
        if metrics.latest_comment_created_utc is not None:
            aggregate_item["LatestCommentCreatedUtc"] = {"N": str(metrics.latest_comment_created_utc)}

        try:
            self._dynamodb.put_item(TableName=self._dynamodb_table_name, Item=aggregate_item)
            print(f"Wrote aggregate metrics to DynamoDB: pk={aggregate_pk} sk={aggregate_sk}")
        except ClientError as e:
            print(f"Failed to write aggregate metrics to DynamoDB: {e}")

        # Write per-subreddit metrics
        for subreddit, sub_metrics in metrics.subreddit_metrics.items():
            pk = f"ENV#{self._environment}#SUBREDDIT#{subreddit}"
            sk = f"GLUE_RUN#{metrics.process_date}#{metrics.run_id}"

            item = {
                "pk": {"S": pk},
                "sk": {"S": sk},
                "Environment": {"S": self._environment},
                "Subreddit": {"S": subreddit},
                "ProcessDate": {"S": metrics.process_date},
                "RunId": {"S": metrics.run_id},
                "SubmissionsCount": {"N": str(sub_metrics.submissions_count)},
                "CommentsCount": {"N": str(sub_metrics.comments_count)},
                "IngestedAtTs": {"N": str(ingested_at_ts)},
            }

            if sub_metrics.earliest_submission_created_utc is not None:
                item["EarliestSubmissionCreatedUtc"] = {"N": str(sub_metrics.earliest_submission_created_utc)}
            if sub_metrics.latest_submission_created_utc is not None:
                item["LatestSubmissionCreatedUtc"] = {"N": str(sub_metrics.latest_submission_created_utc)}
            if sub_metrics.earliest_comment_created_utc is not None:
                item["EarliestCommentCreatedUtc"] = {"N": str(sub_metrics.earliest_comment_created_utc)}
            if sub_metrics.latest_comment_created_utc is not None:
                item["LatestCommentCreatedUtc"] = {"N": str(sub_metrics.latest_comment_created_utc)}

            try:
                self._dynamodb.put_item(TableName=self._dynamodb_table_name, Item=item)
                print(f"Wrote subreddit metrics to DynamoDB: pk={pk} sk={sk}")
            except ClientError as e:
                print(f"Failed to write subreddit metrics to DynamoDB: {e}")

    def _publish_to_cloudwatch(self, metrics: GlueRunMetrics) -> None:
        """Publish aggregate metrics to CloudWatch."""
        dimensions = [
            {"Name": "Environment", "Value": self._environment},
            {"Name": "ProcessDate", "Value": metrics.process_date},
        ]

        metric_data = [
            {
                "MetricName": "SubmissionsProcessed",
                "Dimensions": dimensions,
                "Value": float(metrics.total_submissions),
                "Unit": "Count",
            },
            {
                "MetricName": "CommentsProcessed",
                "Dimensions": dimensions,
                "Value": float(metrics.total_comments),
                "Unit": "Count",
            },
            {
                "MetricName": "SubredditsProcessed",
                "Dimensions": dimensions,
                "Value": float(metrics.subreddits_processed),
                "Unit": "Count",
            },
        ]

        # Add timestamp span metrics if available
        if (
            metrics.earliest_submission_created_utc is not None
            and metrics.latest_submission_created_utc is not None
        ):
            span_seconds = metrics.latest_submission_created_utc - metrics.earliest_submission_created_utc
            metric_data.append({
                "MetricName": "SubmissionTimeSpanSeconds",
                "Dimensions": dimensions,
                "Value": float(span_seconds),
                "Unit": "Seconds",
            })

        if (
            metrics.earliest_comment_created_utc is not None
            and metrics.latest_comment_created_utc is not None
        ):
            span_seconds = metrics.latest_comment_created_utc - metrics.earliest_comment_created_utc
            metric_data.append({
                "MetricName": "CommentTimeSpanSeconds",
                "Dimensions": dimensions,
                "Value": float(span_seconds),
                "Unit": "Seconds",
            })

        try:
            self._cloudwatch.put_metric_data(
                Namespace=self._cloudwatch_namespace,
                MetricData=metric_data,
            )
            print(f"Published {len(metric_data)} metrics to CloudWatch namespace={self._cloudwatch_namespace}")
        except ClientError as e:
            print(f"Failed to publish metrics to CloudWatch: {e}")


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


def compute_submission_metrics(df: DataFrame) -> Dict[str, SubredditMetrics]:
    """Compute per-subreddit metrics for submissions DataFrame."""
    metrics_df = df.groupBy("subreddit").agg(
        spark_count("*").alias("count"),
        spark_min("created_utc").alias("earliest_created_utc"),
        spark_max("created_utc").alias("latest_created_utc"),
    )

    subreddit_metrics = {}
    for row in metrics_df.collect():
        subreddit = row["subreddit"]
        subreddit_metrics[subreddit] = SubredditMetrics(
            subreddit=subreddit,
            submissions_count=row["count"],
            earliest_submission_created_utc=row["earliest_created_utc"],
            latest_submission_created_utc=row["latest_created_utc"],
        )

    return subreddit_metrics


def ensure_iceberg_table_exists(spark, database_name, table_name, sample_df: DataFrame, table_location: str):
    """
    Ensure Iceberg table exists, create it if it doesn't.
    
    Args:
        spark: SparkSession instance
        database_name: Glue catalog database name
        table_name: Table name
        sample_df: Sample DataFrame with the correct schema
        table_location: Explicit S3 location for the table (e.g., s3://bucket/bronze/reddit_submissions/)
    
    Returns:
        True if table exists or was created, False otherwise
    """
    qualified_table = f"glue_catalog.{database_name}.{table_name}"
    
    try:
        # Try to check if table exists by querying it
        spark.sql(f"SELECT 1 FROM {qualified_table} LIMIT 1").collect()
        print(f"Table {qualified_table} already exists with metadata")
        return True
    except Exception as table_error:
        # Table doesn't exist or has no metadata, create it
        error_msg = str(table_error).lower()
        if "table" in error_msg and ("not found" in error_msg or "does not exist" in error_msg or "cannot be null" in error_msg):
            print(f"Table {qualified_table} does not exist or has no metadata. Creating it at {table_location}...")
            try:
                # Get column definitions from schema
                schema = sample_df.schema
                columns = []
                for field in schema.fields:
                    col_type = field.dataType.simpleString()
                    # Map Spark types to SQL types
                    type_mapping = {
                        "string": "string",
                        "bigint": "bigint",
                        "int": "int",
                        "double": "double",
                        "boolean": "boolean",
                        "timestamp": "timestamp"
                    }
                    sql_type = type_mapping.get(col_type, col_type)
                    columns.append(f"`{field.name}` {sql_type}")
                
                columns_str = ", ".join(columns)
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {qualified_table} (
                        {columns_str}
                    )
                    USING ICEBERG
                    LOCATION '{table_location}'
                    TBLPROPERTIES (
                        'write.format.default' = 'parquet',
                        'write.parquet.compression-codec' = 'zstd'
                    )
                """
                
                print(f"Creating table with explicit location: {table_location}")
                spark.sql(create_sql)
                
                print(f"Successfully created Iceberg table {qualified_table} with metadata at {table_location}")
                return True
            except Exception as create_error:
                print(f"Failed to create table {qualified_table}: {create_error}")
                import traceback
                traceback.print_exc()
                return False
        else:
            # Unexpected error, re-raise
            print(f"Unexpected error checking table {qualified_table}: {table_error}")
            raise


def process_submissions(spark, glue_context, landing_path, database_name, table_name, process_date=None):
    """
    Read submissions from Landing Zone and merge into Bronze Iceberg table.
    
    S3 path structure: reddit/{sort_type}/subreddit={name}/date={DD-MM-YYYY}/submissions.jsonl
    
    Args:
        spark: SparkSession instance.
        glue_context: GlueContext instance.
        landing_path: Base S3 path for landing zone data.
        database_name: Glue catalog database name.
        table_name: Target table name.
        process_date: Optional date filter (DD-MM-YYYY). If provided, only processes
                      data from partitions matching this date.
    
    Returns:
        Tuple of (record_count, subreddit_metrics_dict).
    """
    print(f"Processing submissions from: {landing_path}")
    
    schema = get_submissions_schema()
    
    # Use explicit glob pattern matching S3 structure
    submissions_path = f"{landing_path}/*/subreddit=*/date=*/submissions.jsonl"
    print(f"Reading from: {submissions_path}")
    
    df = spark.read.schema(schema).json(submissions_path)
    
    if df.rdd.isEmpty():
        print("No submission files found. Skipping.")
        return 0, {}
    
    df = add_audit_columns(df)
    df = df.drop("awards")
    
    # Filter to only the fresh partition if process_date is specified
    if process_date:
        print(f"Filtering submissions to partition_date = {process_date}")
        df = df.filter(col("partition_date") == lit(process_date))
        
        if df.rdd.isEmpty():
            print(f"No submissions found for partition_date={process_date}. Skipping.")
            return 0, {}

    # Deduplicate the source batch: keep only the latest record per ID
    print("Deduplicating submissions by ID (keeping latest updated_at)...")
    window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())
    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    # Cache the dataframe for metrics computation and MERGE
    df.cache()

    # Compute metrics before the merge
    subreddit_metrics = compute_submission_metrics(df)
    
    record_count = df.count()
    print(f"Found {record_count} submission records to process")
    
    # Ensure table exists before merging
    qualified_table = f"glue_catalog.{database_name}.{table_name}"
    
    # Get warehouse path from Spark config (e.g., s3://bucket/bronze/)
    warehouse_path = spark.conf.get("spark.sql.catalog.glue_catalog.warehouse", "").rstrip("/")
    
    # Table location should match what's defined in Terraform locals
    # locals.tf defines: bronze_submissions_path = "s3://.../bronze/reddit_submissions/"
    # So we need to use the full subdirectory name
    if table_name == "submissions":
        table_location = f"{warehouse_path}/reddit_submissions"
    elif table_name == "comments":
        table_location = f"{warehouse_path}/reddit_comments"
    else:
        table_location = f"{warehouse_path}/{table_name}"
    
    print(f"Table location will be: {table_location}")
    
    if not ensure_iceberg_table_exists(spark, database_name, table_name, df, table_location):
        print(f"Failed to ensure table exists. Attempting direct write...")
        # Fallback: try to create and write in one step with explicit location
        try:
            # Create temp view for CREATE TABLE AS SELECT
            df.createOrReplaceTempView("_temp_write_data")
            create_and_write_sql = f"""
                CREATE TABLE IF NOT EXISTS {qualified_table}
                USING ICEBERG
                LOCATION '{table_location}'
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'zstd'
                )
            """
            spark.sql(create_and_write_sql)
            # Now insert data
            df.sort("partition_date").writeTo(qualified_table).append()
            print(f"Successfully created and wrote {record_count} submissions to {table_name}")
            df.unpersist()
            return record_count, subreddit_metrics
        except Exception as e:
            print(f"Failed to create/write table: {e}")
            import traceback
            traceback.print_exc()
            df.unpersist()
            raise
    
    df.createOrReplaceTempView("submissions_staging")
    
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
        print(f"Executing MERGE for {record_count} submissions...")
        spark.sql(merge_sql)  # MERGE executes immediately, no need for show()
        print(f"MERGE SQL executed successfully.")
        
        # Verify data was written by counting rows
        written_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {qualified_table}").collect()[0]["cnt"]
        print(f"Successfully merged {record_count} submissions into {table_name}. Table now has {written_count} total rows.")
    except Exception as e:
        print(f"MERGE failed: {e}")
        import traceback
        traceback.print_exc()
        print(f"Attempting append instead...")
        try:
            df.sort("partition_date").writeTo(qualified_table).append()
            written_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {qualified_table}").collect()[0]["cnt"]
            print(f"Appended {record_count} submissions to {table_name}. Table now has {written_count} total rows.")
        except Exception as e2:
            print(f"Append also failed: {e2}")
            import traceback
            traceback.print_exc()
            raise

    df.unpersist()
    
    return record_count, subreddit_metrics


def compute_comment_metrics(df: DataFrame) -> Dict[str, Dict[str, any]]:
    """Compute per-subreddit metrics for comments DataFrame.
    
    Note: Comments don't have a direct subreddit field, so we extract it from link_id
    or rely on the source file path. Here we use a simplified approach assuming
    the subreddit info is available via source path or joining later.
    """
    # For comments, we need to extract subreddit from source_file path
    metrics_df = df.withColumn(
        "subreddit_from_path",
        regexp_extract(col("source_file"), r"/subreddit=([^/]+)/", 1)
    ).groupBy("subreddit_from_path").agg(
        spark_count("*").alias("count"),
        spark_min("created_utc").alias("earliest_created_utc"),
        spark_max("created_utc").alias("latest_created_utc"),
    )

    comment_metrics = {}
    for row in metrics_df.collect():
        subreddit = row["subreddit_from_path"]
        comment_metrics[subreddit] = {
            "comments_count": row["count"],
            "earliest_comment_created_utc": row["earliest_created_utc"],
            "latest_comment_created_utc": row["latest_created_utc"],
        }

    return comment_metrics


def process_comments(spark, glue_context, landing_path, database_name, table_name, process_date=None):
    """
    Read comments from Landing Zone and merge into Bronze Iceberg table.
    
    S3 path structure: reddit/{sort_type}/subreddit={name}/date={DD-MM-YYYY}/comments.jsonl
    
    Args:
        spark: SparkSession instance.
        glue_context: GlueContext instance.
        landing_path: Base S3 path for landing zone data.
        database_name: Glue catalog database name.
        table_name: Target table name.
        process_date: Optional date filter (DD-MM-YYYY). If provided, only processes
                      data from partitions matching this date.
    
    Returns:
        Tuple of (record_count, comment_metrics_dict).
    """
    print(f"Processing comments from: {landing_path}")
    
    schema = get_comments_schema()
    
    # Use explicit glob pattern matching S3 structure
    comments_path = f"{landing_path}/*/subreddit=*/date=*/comments.jsonl"
    print(f"Reading from: {comments_path}")
    
    df = spark.read.schema(schema).json(comments_path)
    
    if df.rdd.isEmpty():
        print("No comment files found. Skipping.")
        return 0, {}
    
    df = add_audit_columns(df)
    df = df.drop("awards")
    
    # Filter to only the fresh partition if process_date is specified
    if process_date:
        print(f"Filtering comments to partition_date = {process_date}")
        df = df.filter(col("partition_date") == lit(process_date))
        
        if df.rdd.isEmpty():
            print(f"No comments found for partition_date={process_date}. Skipping.")
            return 0, {}

    # Deduplicate the source batch: keep only the latest record per ID
    print("Deduplicating comments by ID (keeping latest updated_at)...")
    window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())
    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    # Cache the dataframe for metrics computation and MERGE
    df.cache()

    # Compute metrics before the merge
    comment_metrics = compute_comment_metrics(df)
    
    record_count = df.count()
    print(f"Found {record_count} comment records to process")
    
    # Ensure table exists before merging
    qualified_table = f"glue_catalog.{database_name}.{table_name}"
    # Determine table location based on table name
    warehouse_path = spark.conf.get("spark.sql.catalog.glue_catalog.warehouse", "").rstrip("/")
    if table_name == "submissions":
        table_location = f"{warehouse_path}/reddit_submissions"
    elif table_name == "comments":
        table_location = f"{warehouse_path}/reddit_comments"
    else:
        table_location = f"{warehouse_path}/{table_name}"
    
    if not ensure_iceberg_table_exists(spark, database_name, table_name, df, table_location):
        print(f"Failed to ensure table exists. Attempting direct write...")
        # Fallback: try to create and write in one step with explicit location
        try:
            # Create temp view for CREATE TABLE AS SELECT
            df.createOrReplaceTempView("_temp_write_data")
            create_and_write_sql = f"""
                CREATE TABLE IF NOT EXISTS {qualified_table}
                USING ICEBERG
                LOCATION '{table_location}'
                TBLPROPERTIES (
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'zstd'
                )
            """
            spark.sql(create_and_write_sql)
            # Now insert data
            df.sort("partition_date").writeTo(qualified_table).append()
            print(f"Successfully created and wrote {record_count} comments to {table_name}")
            df.unpersist()
            return record_count, comment_metrics
        except Exception as e:
            print(f"Failed to create/write table: {e}")
            import traceback
            traceback.print_exc()
            df.unpersist()
            raise
    
    df.createOrReplaceTempView("comments_staging")
    
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
        print(f"Executing MERGE for {record_count} comments...")
        spark.sql(merge_sql)  # MERGE executes immediately, no need for show()
        print(f"MERGE SQL executed successfully.")
        
        # Verify data was written by counting rows
        written_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {qualified_table}").collect()[0]["cnt"]
        print(f"Successfully merged {record_count} comments into {table_name}. Table now has {written_count} total rows.")
    except Exception as e:
        print(f"MERGE failed: {e}")
        import traceback
        traceback.print_exc()
        print(f"Attempting append instead...")
        try:
            df.sort("partition_date").writeTo(qualified_table).append()
            written_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {qualified_table}").collect()[0]["cnt"]
            print(f"Appended {record_count} comments to {table_name}. Table now has {written_count} total rows.")
        except Exception as e2:
            print(f"Append also failed: {e2}")
            import traceback
            traceback.print_exc()
            raise

    df.unpersist()
    
    return record_count, comment_metrics


def merge_metrics(
    submission_metrics: Dict[str, SubredditMetrics],
    comment_metrics: Dict[str, Dict[str, any]],
    process_date: str,
    run_id: str,
) -> GlueRunMetrics:
    """Merge submission and comment metrics into a single GlueRunMetrics object."""
    all_subreddits = set(submission_metrics.keys()) | set(comment_metrics.keys())

    merged_subreddit_metrics = {}
    total_submissions = 0
    total_comments = 0
    earliest_sub_ts = None
    latest_sub_ts = None
    earliest_com_ts = None
    latest_com_ts = None

    for subreddit in all_subreddits:
        sub_m = submission_metrics.get(subreddit)
        com_m = comment_metrics.get(subreddit, {})

        submissions_count = sub_m.submissions_count if sub_m else 0
        comments_count = com_m.get("comments_count", 0)

        earliest_sub = sub_m.earliest_submission_created_utc if sub_m else None
        latest_sub = sub_m.latest_submission_created_utc if sub_m else None
        earliest_com = com_m.get("earliest_comment_created_utc")
        latest_com = com_m.get("latest_comment_created_utc")

        merged_subreddit_metrics[subreddit] = SubredditMetrics(
            subreddit=subreddit,
            submissions_count=submissions_count,
            comments_count=comments_count,
            earliest_submission_created_utc=earliest_sub,
            latest_submission_created_utc=latest_sub,
            earliest_comment_created_utc=earliest_com,
            latest_comment_created_utc=latest_com,
        )

        total_submissions += submissions_count
        total_comments += comments_count

        # Update global min/max
        if earliest_sub is not None:
            if earliest_sub_ts is None or earliest_sub < earliest_sub_ts:
                earliest_sub_ts = earliest_sub
        if latest_sub is not None:
            if latest_sub_ts is None or latest_sub > latest_sub_ts:
                latest_sub_ts = latest_sub
        if earliest_com is not None:
            if earliest_com_ts is None or earliest_com < earliest_com_ts:
                earliest_com_ts = earliest_com
        if latest_com is not None:
            if latest_com_ts is None or latest_com > latest_com_ts:
                latest_com_ts = latest_com

    return GlueRunMetrics(
        process_date=process_date,
        run_id=run_id,
        total_submissions=total_submissions,
        total_comments=total_comments,
        subreddits_processed=len(all_subreddits),
        earliest_submission_created_utc=earliest_sub_ts,
        latest_submission_created_utc=latest_sub_ts,
        earliest_comment_created_utc=earliest_com_ts,
        latest_comment_created_utc=latest_com_ts,
        subreddit_metrics=merged_subreddit_metrics,
    )


def main():
    # Required parameters
    required_params = [
        'JOB_NAME',
        'LANDING_BUCKET',
        'DATABASE_NAME',
        'AUGMENTED_BUCKET',
        'METRICS_TABLE_NAME',
        'ENVIRONMENT',
        'METRICS_NAMESPACE',
    ]
    
    # Check if optional PROCESS_DATE is provided in command line args
    has_process_date = any(arg.startswith('--PROCESS_DATE') for arg in sys.argv)
    if has_process_date:
        required_params.append('PROCESS_DATE')
    
    args = getResolvedOptions(sys.argv, required_params)
    
    job_name = args['JOB_NAME']
    landing_bucket = args['LANDING_BUCKET']
    database_name = args['DATABASE_NAME']
    augmented_bucket = args['AUGMENTED_BUCKET']
    metrics_table_name = args['METRICS_TABLE_NAME']
    environment = args['ENVIRONMENT']
    metrics_namespace = args['METRICS_NAMESPACE']
    
    # Use provided PROCESS_DATE if available, otherwise None (process all)
    process_date = args.get('PROCESS_DATE')
    if not process_date:
        print("No PROCESS_DATE provided. Processing ALL available data in Landing Zone.")
    else:
        print(f"PROCESS_DATE provided: {process_date}. Filtering data to this partition.")
    
    # Generate a unique run ID for this job execution
    run_id = str(uuid.uuid4())[:8]
    
    landing_path = f"s3://{landing_bucket}/reddit"
    
    print(f"Starting Bronze ingestion job: {job_name}")
    print(f"Landing path: {landing_path}")
    print(f"Database: {database_name}")
    print(f"Process date filter: {process_date}")
    print(f"Run ID: {run_id}")
    print(f"Metrics table: {metrics_table_name}")
    print(f"Environment: {environment}")
    
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{augmented_bucket}/bronze/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(job_name, args)
    
    submissions_count, submission_metrics = process_submissions(
        spark, glue_context, landing_path,
        database_name, "submissions",
        process_date=process_date
    )
    
    comments_count, comment_metrics = process_comments(
        spark, glue_context, landing_path,
        database_name, "comments",
        process_date=process_date
    )
    
    print(f"Job completed. Submissions: {submissions_count}, Comments: {comments_count}")
    
    # Merge and write metrics
    run_metrics = merge_metrics(
        submission_metrics=submission_metrics,
        comment_metrics=comment_metrics,
        process_date=process_date,
        run_id=run_id,
    )
    
    print(f"Writing metrics: subreddits={run_metrics.subreddits_processed}, "
          f"submissions={run_metrics.total_submissions}, comments={run_metrics.total_comments}")
    
    metrics_writer = GlueMetricsWriter(
        dynamodb_table_name=metrics_table_name,
        cloudwatch_namespace=metrics_namespace,
        environment=environment,
    )
    metrics_writer.write_metrics(run_metrics)
    
    job.commit()


if __name__ == "__main__":
    main()

