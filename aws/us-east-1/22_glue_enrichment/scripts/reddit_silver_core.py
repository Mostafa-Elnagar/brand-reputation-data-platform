"""
Reddit Silver-core Layer Cleaning Glue Job.

This script reads Bronze layer Iceberg tables and creates cleaned Silver-core tables
with computed `is_usable_for_sentiment` flags. The job preserves all rows (no deletions)
to maintain tree structure and lineage, but flags low-quality content for exclusion
from downstream sentiment analysis.

Cleaning Rules:
- Submissions: Flag as unusable if selftext is deleted/removed or content is too short
- Comments: Flag as unusable if body is deleted/removed or content is too short
- All rows are retained for context preservation

Metrics are collected and emitted to CloudWatch for monitoring data quality.

Usage:
    Triggered via AWS Glue Job with the following parameters:
    --LAKEHOUSE_BUCKET: S3 bucket for Iceberg warehouse
    --BRONZE_DATABASE: Glue catalog database name for Bronze tables
    --SILVER_DATABASE: Glue catalog database name for Silver tables
    --PROCESS_DATE: Date filter (DD-MM-YYYY format) for partition processing
    --MIN_SUBMISSION_LEN_CHARS: Minimum character length for submissions (default: 10)
    --MIN_SUBMISSION_MIN_WORDS: Minimum word count for submissions (default: 2)
    --MIN_COMMENT_LEN_CHARS: Minimum character length for comments (default: 30)
    --MIN_COMMENT_MIN_WORDS: Minimum word count for comments (default: 5)
    --ENVIRONMENT: Environment name (e.g., prod, dev)
"""

import sys
from datetime import datetime, timezone
from typing import Dict

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, length, size, split, trim, when,
    count as spark_count, sum as spark_sum
)

# =============================================================================
# Configuration and Logging
# =============================================================================

required_params = [
    'JOB_NAME',
    'LAKEHOUSE_BUCKET',
    'BRONZE_DATABASE',
    'SILVER_DATABASE',
    'ENVIRONMENT',
]

# Add optional parameters if present in sys.argv
if any('--PROCESS_DATE' in arg for arg in sys.argv):
    required_params.append('PROCESS_DATE')
if any('--MIN_SUBMISSION_LEN_CHARS' in arg for arg in sys.argv):
    required_params.append('MIN_SUBMISSION_LEN_CHARS')
if any('--MIN_SUBMISSION_MIN_WORDS' in arg for arg in sys.argv):
    required_params.append('MIN_SUBMISSION_MIN_WORDS')
if any('--MIN_COMMENT_LEN_CHARS' in arg for arg in sys.argv):
    required_params.append('MIN_COMMENT_LEN_CHARS')
if any('--MIN_COMMENT_MIN_WORDS' in arg for arg in sys.argv):
    required_params.append('MIN_COMMENT_MIN_WORDS')

args = getResolvedOptions(sys.argv, required_params)

# Optional arguments with defaults
PROCESS_DATE = args.get('PROCESS_DATE')
MIN_SUBMISSION_LEN_CHARS = int(args.get('MIN_SUBMISSION_LEN_CHARS', '10'))
MIN_SUBMISSION_MIN_WORDS = int(args.get('MIN_SUBMISSION_MIN_WORDS', '2'))
MIN_COMMENT_LEN_CHARS = int(args.get('MIN_COMMENT_LEN_CHARS', '30'))
MIN_COMMENT_MIN_WORDS = int(args.get('MIN_COMMENT_MIN_WORDS', '5'))

# Initialize Spark/Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['LAKEHOUSE_BUCKET']}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

logger = glueContext.get_logger()
logger.info(f"Starting Silver-core cleaning job for PROCESS_DATE: {PROCESS_DATE}")
logger.info(f"Thresholds - Submission: {MIN_SUBMISSION_LEN_CHARS} chars, {MIN_SUBMISSION_MIN_WORDS} words")
logger.info(f"Thresholds - Comment: {MIN_COMMENT_LEN_CHARS} chars, {MIN_COMMENT_MIN_WORDS} words")

# =============================================================================
# CloudWatch Metrics Helper
# =============================================================================

class CloudWatchMetricsWriter:
    """Simplified CloudWatch metrics writer for Silver-core job."""
    
    def __init__(self, namespace: str, environment: str):
        self.namespace = namespace
        self.environment = environment
        self.cloudwatch = boto3.client('cloudwatch')
        self.metrics = []
    
    def add_metric(self, metric_name: str, value: float, unit: str = 'Count'):
        """Add a metric to be published."""
        self.metrics.append({
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.now(timezone.utc),
            'Dimensions': [
                {'Name': 'Environment', 'Value': self.environment},
                {'Name': 'JobName', 'Value': args['JOB_NAME']},
            ]
        })
    
    def publish(self):
        """Publish all collected metrics to CloudWatch."""
        if not self.metrics:
            return
        
        try:
            # CloudWatch allows max metrics per request
            for i in range(0, len(self.metrics), CLOUDWATCH_MAX_METRICS_PER_BATCH):
                batch = self.metrics[i:i+CLOUDWATCH_MAX_METRICS_PER_BATCH]
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
            logger.info(f"Published {len(self.metrics)} metrics to CloudWatch")
        except Exception as e:
            logger.error(f"Failed to publish CloudWatch metrics: {e}")

# =============================================================================
# Constants
# =============================================================================

CLOUDWATCH_MAX_METRICS_PER_BATCH = 20

# =============================================================================
# Helper Functions
# =============================================================================

def count_words_col(col_name: str):
    """Helper to count words in a column, handling empty strings."""
    return when(
        trim(col(col_name)) == "",
        lit(0)
    ).otherwise(
        size(split(trim(col(col_name)), r"\s+"))
    )

# =============================================================================
# Silver-core Cleaning Functions
# =============================================================================

def clean_submissions(bronze_df: DataFrame) -> DataFrame:
    """
    Clean submissions from Bronze and compute is_usable_for_sentiment flag.
    
    Args:
        bronze_df: Bronze submissions DataFrame
        
    Returns:
        Cleaned Silver-core submissions DataFrame with is_usable_for_sentiment flag
    """
    logger.info("Processing submissions for Silver-core layer")
    
    # Select relevant columns
    submissions = bronze_df.select(
        col("id"),
        col("fullname_id"),
        col("subreddit"),
        col("title"),
        col("selftext"),
        col("created_utc"),
        col("score"),
        col("upvote_ratio"),
        col("num_comments"),
        col("partition_date")
    )
    
    # Compute is_usable_for_sentiment flag
    # Unusable if:
    # 1. selftext is deleted/removed/empty
    # 2. Combined title + selftext is below thresholds
    submissions = submissions.withColumn(
        "is_usable_for_sentiment",
        when(
            col("selftext").isin('[deleted]', '[removed]', ''),
            lit(False)
        ).when(
            # Check combined content length
            (length(trim(col("title"))) + length(trim(col("selftext")))) < MIN_SUBMISSION_LEN_CHARS,
            lit(False)
        ).when(
            # Check combined word count (with proper empty string handling)
            (count_words_col("title") + count_words_col("selftext")) < MIN_SUBMISSION_MIN_WORDS,
            lit(False)
        ).otherwise(lit(True))
    )
    
    logger.info("Processed submissions from Bronze layer")
    return submissions


def clean_comments(bronze_df: DataFrame, submissions_df: DataFrame) -> DataFrame:
    """
    Clean comments from Bronze and compute is_usable_for_sentiment flag.
    
    Args:
        bronze_df: Bronze comments DataFrame
        submissions_df: Bronze submissions DataFrame (to derive subreddit)
        
    Returns:
        Cleaned Silver-core comments DataFrame with is_usable_for_sentiment flag
    """
    logger.info("Processing comments for Silver-core layer")
    
    # Select relevant columns from comments
    comments = bronze_df.select(
        col("id"),
        col("link_id"),
        col("parent_id"),
        col("body"),
        col("score"),
        col("created_utc"),
        col("partition_date")
    )
    
    # Join with submissions to get subreddit
    # link_id format is "t3_<submission_id>", fullname_id format is also "t3_<submission_id>"
    submissions_minimal = submissions_df.select(
        col("fullname_id"),
        col("subreddit")
    )
    
    comments = comments.join(
        submissions_minimal,
        comments.link_id == submissions_minimal.fullname_id,
        "left"
    ).select(
        comments["*"],
        submissions_minimal["subreddit"]
    )
    
    # Compute is_usable_for_sentiment flag
    # Unusable if:
    # 1. body is deleted/removed/empty
    # 2. body is below thresholds
    comments = comments.withColumn(
        "is_usable_for_sentiment",
        when(
            col("body").isin('[deleted]', '[removed]', ''),
            lit(False)
        ).when(
            length(trim(col("body"))) < MIN_COMMENT_LEN_CHARS,
            lit(False)
        ).when(
            # Check word count with proper empty string handling
            count_words_col("body") < MIN_COMMENT_MIN_WORDS,
            lit(False)
        ).otherwise(lit(True))
    )
    
    logger.info("Processed comments from Bronze layer")
    return comments


def compute_metrics(submissions_df: DataFrame, comments_df: DataFrame) -> Dict[str, float]:
    """
    Compute data quality metrics for monitoring.
    
    Args:
        submissions_df: Cleaned submissions DataFrame
        comments_df: Cleaned comments DataFrame
        
    Returns:
        Dictionary of metric names to values
    """
    logger.info("Computing data quality metrics")
    
    # Submissions metrics
    sub_stats = submissions_df.agg(
        spark_count("*").alias("total"),
        spark_sum(when(col("is_usable_for_sentiment"), 1).otherwise(0)).alias("usable")
    ).collect()[0]
    
    submissions_total = sub_stats["total"]
    submissions_usable = sub_stats["usable"] or 0
    
    # Comments metrics
    com_stats = comments_df.agg(
        spark_count("*").alias("total"),
        spark_sum(when(col("is_usable_for_sentiment"), 1).otherwise(0)).alias("usable")
    ).collect()[0]
    
    comments_total = com_stats["total"]
    comments_usable = com_stats["usable"] or 0
    
    metrics = {
        "SubmissionsTotal": float(submissions_total),
        "SubmissionsUsableForSentiment": float(submissions_usable),
        "SubmissionsUsableRatio": float(submissions_usable) / float(submissions_total) if submissions_total > 0 else 0.0,
        "CommentsTotal": float(comments_total),
        "CommentsUsableForSentiment": float(comments_usable),
        "CommentsUsableRatio": float(comments_usable) / float(comments_total) if comments_total > 0 else 0.0,
    }
    
    logger.info(f"Metrics computed: {metrics}")
    return metrics


# =============================================================================
# Main Processing Logic
# =============================================================================

try:
    start_time = datetime.now(timezone.utc)
    logger.info(f"Job started at {start_time}")
    
    # Initialize metrics writer
    metrics_writer = CloudWatchMetricsWriter(
        namespace="RedditSilverCore",
        environment=args['ENVIRONMENT']
    )
    
    # Build partition filter if PROCESS_DATE is provided
    partition_filter = f"partition_date = '{PROCESS_DATE}'" if PROCESS_DATE else None
    
    # Read Bronze tables using catalog prefix
    bronze_submissions_table = f"glue_catalog.{args['BRONZE_DATABASE']}.submissions"
    bronze_comments_table = f"glue_catalog.{args['BRONZE_DATABASE']}.comments"
    
    logger.info(f"Reading Bronze submissions from {bronze_submissions_table}")
    if partition_filter:
        bronze_submissions = spark.table(bronze_submissions_table) \
            .filter(partition_filter)
        logger.info(f"Applied partition filter: {partition_filter}")
    else:
        bronze_submissions = spark.table(bronze_submissions_table)
    
    logger.info(f"Reading Bronze comments from {bronze_comments_table}")
    if partition_filter:
        bronze_comments = spark.table(bronze_comments_table) \
            .filter(partition_filter)
    else:
        bronze_comments = spark.table(bronze_comments_table)
    
    # Check for empty input data
    bronze_sub_count = bronze_submissions.count()
    bronze_com_count = bronze_comments.count()
    
    if bronze_sub_count == 0 and bronze_com_count == 0:
        if PROCESS_DATE:
            logger.warn(f"No data found for PROCESS_DATE = {PROCESS_DATE}. Skipping processing.")
        else:
            logger.warn("No data found in Bronze layer. Skipping processing.")
    else:
        logger.info(f"Processing {bronze_sub_count} submissions and {bronze_com_count} comments from Bronze")

        # Process submissions
        silver_submissions = clean_submissions(bronze_submissions)
        
        # Process comments (pass submissions for subreddit join)
        silver_comments = clean_comments(bronze_comments, bronze_submissions)
        
        # Compute metrics before writing
        metrics = compute_metrics(silver_submissions, silver_comments)
        for metric_name, value in metrics.items():
            metrics_writer.add_metric(metric_name, value)
        
        # Write to Silver-core Iceberg tables
        # Use overwritePartitions() for idempotent, incremental updates.
        # This replaces data ONLY for the partitions present in the source DataFrame.
        silver_submissions_table = f"glue_catalog.{args['SILVER_DATABASE']}.submissions_silver"
        silver_comments_table = f"glue_catalog.{args['SILVER_DATABASE']}.comments_silver"

        logger.info(f"Writing submissions to {silver_submissions_table}")
        # Ensure table exists before writing (create if not exists)
        try:
            spark.sql(f"SELECT 1 FROM {silver_submissions_table} LIMIT 1")
        except Exception:
            logger.info(f"Table {silver_submissions_table} does not exist. Creating it.")
            silver_submissions.sort("partition_date").writeTo(silver_submissions_table) \
                .using("iceberg") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("write.parquet.compression-codec", "zstd") \
                .partitionedBy("partition_date") \
                .createOrReplace()
        else:
            # Table exists, use overwritePartitions
            silver_submissions.sort("partition_date").writeTo(silver_submissions_table) \
                .overwritePartitions()
        
        logger.info(f"Writing comments to {silver_comments_table}")
        # Ensure table exists before writing
        try:
            spark.sql(f"SELECT 1 FROM {silver_comments_table} LIMIT 1")
        except Exception:
            logger.info(f"Table {silver_comments_table} does not exist. Creating it.")
            silver_comments.sort("partition_date").writeTo(silver_comments_table) \
                .using("iceberg") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("write.parquet.compression-codec", "zstd") \
                .partitionedBy("partition_date") \
                .createOrReplace()
        else:
            # Table exists, use overwritePartitions
            silver_comments.sort("partition_date").writeTo(silver_comments_table) \
                .overwritePartitions()
        
        # Publish metrics
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        metrics_writer.add_metric("JobDurationSeconds", duration_seconds, unit='Seconds')
        metrics_writer.publish()
        
        logger.info(f"Silver-core cleaning job completed successfully in {duration_seconds:.2f} seconds")
        logger.info(f"Submissions: {metrics['SubmissionsTotal']} total, "
                    f"{metrics['SubmissionsUsableForSentiment']} usable "
                    f"({metrics['SubmissionsUsableRatio']:.1%})")
        logger.info(f"Comments: {metrics['CommentsTotal']} total, "
                    f"{metrics['CommentsUsableForSentiment']} usable "
                    f"({metrics['CommentsUsableRatio']:.1%})")

except Exception as e:
    logger.error(f"Job failed with error: {str(e)}")
    raise

finally:
    job.commit()
    spark.stop()
