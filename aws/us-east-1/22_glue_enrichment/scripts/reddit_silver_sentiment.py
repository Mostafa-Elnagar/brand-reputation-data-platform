import sys
import boto3
import json
import logging
from typing import Iterator
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    struct,
    current_timestamp,
    lower,
    when,
    hash as spark_hash
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Import custom modules
# Note: These must be zipped and passed via --extra-py-files
from sentiment_analysis.gemini_client import GeminiClient
from sentiment_analysis.prompt_builder import PromptBuilder
from sentiment_analysis.normalizer import ModelNormalizer
import pandas as pd


# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'LAKEHOUSE_BUCKET',
        'SILVER_DATABASE',
        'GEMINI_SECRET_ARN',
        'ENVIRONMENT',
        'REF_DATA_S3_PATH',
    ],
)

# Optional arguments
PROCESS_DATE = args.get('PROCESS_DATE')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Iceberg catalog
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['LAKEHOUSE_BUCKET']}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# =============================================================================
# Helper Functions
# =============================================================================

def get_secret(secret_arn: str) -> list:
    """Fetch API keys from AWS Secrets Manager.
    
    Expects secret to be a JSON array: ["key1", "key2", "key3"]
    Falls back to single key string for backward compatibility.
    """
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    secret_string = response['SecretString']
    
    try:
        # Try parsing as JSON array
        keys = json.loads(secret_string)
        if isinstance(keys, list):
            logger.info(f"Loaded {len(keys)} API keys from Secrets Manager")
            return keys
        else:
            # Single key in JSON format {"key": "value"}
            logger.warning("Secret is not a JSON array, treating as single key")
            return [secret_string]
    except json.JSONDecodeError:
        # Plain string key (backward compatibility)
        logger.warning("Secret is plain string, wrapping as single-key list")
        return [secret_string]


def load_reference_data(spark: SparkSession, database: str, ref_data_s3_path: str) -> pd.DataFrame:
    """
    Load reference data for smartphones into a Pandas DataFrame.

    Priority:
    1. Read from Silver Iceberg table dim_products_ref if it exists and has data.
    2. Otherwise, seed from the CSV in S3 (uploaded via Terraform) and write into dim_products_ref.
    
    Args:
        spark: SparkSession instance
        database: Silver database name
        ref_data_s3_path: Full S3 path to the reference CSV file
    """
    table_fqn = f"glue_catalog.{database}.dim_products_ref"

    # Try reading existing Iceberg table
    try:
        existing_df = spark.table(table_fqn)
        if existing_df.count() > 0:
            logger.info("Loaded reference data from existing dim_products_ref Iceberg table")
            # Return full dataframe as pandas, but specifically we need 'brand' and 'model' for normalizer
            # The normalizer might use other columns if we enhanced it, but for now it uses 'brand' and 'model'
            # We return full DF to be flexible
            return existing_df.toPandas()
        logger.warning("dim_products_ref table exists but is empty. Seeding from CSV.")
    except Exception as exc:
        logger.info(f"dim_products_ref not found or unreadable ({exc}). Seeding from CSV.")

    # Seed from CSV uploaded to S3
    logger.info(f"Loading reference CSV from {ref_data_s3_path}")

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")  # Infer data types from CSV
        .csv(ref_data_s3_path)
    )

    # Ingest all columns as-is, only add a lowercase 'brand' column for fuzzy matching
    # The normalizer expects lowercase brand names for matching
    ref_df = raw_df.withColumn("brand", lower(col("brand_name")))

    logger.info("Writing seeded reference data into dim_products_ref Iceberg table")
    (
        ref_df.writeTo(table_fqn)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .using("iceberg")
        .createOrReplace()
    )

    return ref_df.toPandas()


# =============================================================================
# Constants
# =============================================================================

CLOUDWATCH_MAX_METRICS_PER_BATCH = 20

# =============================================================================
# CloudWatch Metrics Helper
# =============================================================================

class CloudWatchMetricsWriter:
    """Simplified CloudWatch metrics writer for Silver-sentiment job."""
    
    def __init__(self, namespace: str, environment: str):
        self.namespace = namespace
        self.environment = environment
        self.cloudwatch = boto3.client('cloudwatch')
        self.metrics = []
    
    def add_metric(self, metric_name: str, value: float, unit: str = 'Count'):
        """Add a metric to be published."""
        from datetime import timezone
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
# Main Processing Logic
# =============================================================================

def process_batch(partition_data: Iterator[pd.DataFrame], gemini_keys: list, ref_df_broadcast) -> Iterator[pd.DataFrame]:
    """
    Process a partition of data asynchronously using Gemini API.
    """
    import asyncio
    import logging
    import sys
    
    # Configure logging on executor
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    print(f"Executor: Starting process_batch for partition")
    logger.info("Executor: Initializing components")
    
    # Initialize components per executor
    client = GeminiClient(api_keys=gemini_keys)
    ref_data = ref_df_broadcast.value
    
    unique_brands = ref_data['brand'].unique().tolist() if not ref_data.empty else []
    prompt_builder = PromptBuilder(reference_brands=unique_brands)
    normalizer = ModelNormalizer(reference_data=ref_data)

    async def process_row_async(row, semaphore):
        async with semaphore:
            try:
                prompt = prompt_builder.build_analysis_prompt(
                    comment_body=row['body'],
                    submission_title=row['submission_title'],
                    submission_selftext=row['submission_selftext'],
                    parent_body=row.get('parent_body')
                )
                
                response_json = await client.analyze_sentiment_async(prompt, system_prompt=prompt_builder.system_prompt)
                
                if not response_json:
                    return []

                # Parse JSON
                try:
                    cleaned_json = response_json.strip()
                    if cleaned_json.startswith("```json"):
                        cleaned_json = cleaned_json[7:]
                    if cleaned_json.startswith("```"):
                        cleaned_json = cleaned_json[3:]
                    if cleaned_json.endswith("```"):
                        cleaned_json = cleaned_json[:-3]
                    cleaned_json = cleaned_json.strip()

                    analysis_list = json.loads(cleaned_json)
                    if not isinstance(analysis_list, list):
                        return []
                        
                except json.JSONDecodeError:
                    return []
                
                row_results = []
                for item in analysis_list:
                    norm = normalizer.normalize(item.get('brand_name', ''), item.get('product_name', ''))
                    quote = item.get('quote', '')
                    if not quote:
                        quote = row['body'][:150] + "..." if len(row['body']) > 150 else row['body']
                    
                    from datetime import timezone
                    row_results.append({
                        'submission_id': row['submission_id'],
                        'comment_id': row['comment_id'],
                        'brand_name': norm['brand_name'],
                        'product_name': norm['category'],
                        'aspect': item.get('aspect', 'general').lower(),
                        'sentiment_score': float(item.get('sentiment_score', 0.5)),
                        'quote': quote[:200],
                        'model_version': client.model_name,
                        'analyzed_at': datetime.now(timezone.utc),
                        'date_partition': row['partition_date']
                    })
                return row_results
                
            except Exception as e:
                logger.error(f"Error processing comment {row.get('comment_id')}: {e}")
                return []

    async def process_partition_async(pdf):
        import time
        results = []
        # Limit concurrency to 15 requests at a time
        semaphore = asyncio.Semaphore(15)
        
        tasks = []
        rows = [row for _, row in pdf.iterrows()]
        total_rows = len(rows)
        
        logger.info(f"Starting batch of {total_rows} comments...")
        batch_start_time = time.time()
        
        # Create tasks
        for row in rows:
            tasks.append(process_row_async(row, semaphore))
        
        # Gather results with progress tracking
        # Since asyncio.gather waits for all, we can't easily track individual completion without wrapping
        # So we'll use as_completed for progress bar
        
        completed_count = 0
        batch_results = []
        
        for future in asyncio.as_completed(tasks):
            res = await future
            batch_results.append(res)
            completed_count += 1
            
            # Progress bar logic
            if total_rows >= 10:
                if completed_count % (total_rows // 10) == 0 or completed_count == total_rows:
                    percent = int((completed_count / total_rows) * 100)
                    logger.info(f"Progress: [{completed_count}/{total_rows}] {percent}% completed")
        
        batch_duration = time.time() - batch_start_time
        logger.info(f"Finished API calls in {batch_duration:.2f}s. Preparing results...")
        
        # Flatten results and count successes
        success_count = 0
        for res in batch_results:
            if res:
                success_count += 1
                results.extend(res)
        
        logger.info(f"Generated {len(results)} sentiment records from {success_count} successful API calls.")
            
        return results

    for pdf in partition_data:
        # Run async processing for this pandas dataframe chunk
        results = asyncio.run(process_partition_async(pdf))
        
        if results:
            yield pd.DataFrame(results)

# =============================================================================
# Spark Job Execution
# =============================================================================

# =============================================================================
# Spark Job Execution
# =============================================================================

try:
    from datetime import timezone
    start_time = datetime.now(timezone.utc)
    logger.info(f"Silver-sentiment job started at {start_time}")
    
    # Initialize metrics writer
    metrics_writer = CloudWatchMetricsWriter(
        namespace="RedditSilverSentiment",
        environment=args['ENVIRONMENT']
    )
    
    # 1. Load Data from Silver-core tables using catalog prefix
    submissions_table = f"glue_catalog.{args['SILVER_DATABASE']}.submissions_silver"
    comments_table = f"glue_catalog.{args['SILVER_DATABASE']}.comments_silver"
    
    logger.info(f"Reading Silver-core submissions from {submissions_table}")
    if PROCESS_DATE:
        submissions_df = spark.table(submissions_table) \
            .filter(f"partition_date = '{PROCESS_DATE}'")
        logger.info(f"Applied partition filter: partition_date = '{PROCESS_DATE}'")
    else:
        submissions_df = spark.table(submissions_table)
    
    logger.info(f"Reading Silver-core comments from {comments_table}")
    if PROCESS_DATE:
        comments_df = spark.table(comments_table) \
            .filter(f"partition_date = '{PROCESS_DATE}'")
    else:
        comments_df = spark.table(comments_table)
    
    # 2. Filter comments to only those usable for sentiment analysis
    logger.info("Filtering comments where is_usable_for_sentiment = true")
    usable_comments_df = comments_df.filter(col("is_usable_for_sentiment") == lit(True))
    
    # 3. Denormalize Context
    # Join comments with submissions to get title/selftext
    # Also get subreddit from submission to iterate over
    context_df = (
        usable_comments_df.alias("c")
        .join(
            submissions_df.alias("s"),
            col("c.link_id") == col("s.fullname_id"),
            "inner",
        )
        .select(
            col("c.id").alias("comment_id"),
            col("c.body"),
            col("c.parent_id"),
            col("s.id").alias("submission_id"),
            col("s.title").alias("submission_title"),
            col("s.selftext").alias("submission_selftext"),
            col("s.subreddit"),  # Added subreddit for iteration
            col("c.partition_date"),
        )
    )

    # Cache context_df as we'll use it multiple times
    context_df.cache()
    
    total_comments = context_df.count()
    if total_comments == 0:
        logger.warning(f"No usable comments found for PROCESS_DATE = {PROCESS_DATE}. Skipping processing.")
        job.commit()
        sys.exit(0)
        
    logger.info(f"Total usable comments to process: {total_comments}")

    # 4. Prepare for UDF/MapPartitions
    logger.info("Retrieving Gemini API keys from Secrets Manager")
    gemini_keys = get_secret(args['GEMINI_SECRET_ARN'])
    logger.info(f"Using {len(gemini_keys)} API keys for load distribution")
    
    # 5. Ensure and Broadcast Reference Data
    ref_df_pd = load_reference_data(
        spark=spark,
        database=args['SILVER_DATABASE'],
        ref_data_s3_path=args['REF_DATA_S3_PATH'],
    )
    ref_broadcast = sc.broadcast(ref_df_pd)
    
    # 6. Get list of subreddits to process iteratively
    subreddits = [row.subreddit for row in context_df.select("subreddit").distinct().collect()]
    logger.info(f"Found {len(subreddits)} subreddits to process: {subreddits}")
    
    sentiment_table = f"glue_catalog.{args['SILVER_DATABASE']}.sentiment_analysis"
    
    # Ensure table exists first (create empty if needed)
    # This is needed so we can append in the loop
    try:
        spark.sql(f"SELECT 1 FROM {sentiment_table} LIMIT 1")
    except Exception:
        logger.info(f"Table {sentiment_table} does not exist. Creating it first.")
        # Create empty table with correct schema
        empty_schema = StructType([
            StructField("submission_id", StringType(), True),
            StructField("comment_id", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("aspect", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("quote", StringType(), True),
            StructField("model_version", StringType(), True),
            StructField("analyzed_at", TimestampType(), True),
            StructField("date_partition", StringType(), True),
            StructField("analysis_id", StringType(), True)
        ])
        spark.createDataFrame([], empty_schema).writeTo(sentiment_table) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "zstd") \
            .partitionedBy("date_partition") \
            .createOrReplace()

    processed_count = 0
    
    # 7. Process each subreddit iteratively
    for subreddit in subreddits:
        logger.info(f"--- Starting processing for subreddit: {subreddit} ---")
        
        # Filter for current subreddit
        subreddit_df = context_df.filter(col("subreddit") == lit(subreddit))
        subreddit_count = subreddit_df.count()
        logger.info(f"Subreddit {subreddit} has {subreddit_count} comments to analyze")
        
        if subreddit_count == 0:
            continue
            
        # Apply Analysis
        schema = StructType([
            StructField("submission_id", StringType(), True),
            StructField("comment_id", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("aspect", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("quote", StringType(), True),
            StructField("model_version", StringType(), True),
            StructField("analyzed_at", TimestampType(), True),
            StructField("date_partition", StringType(), True),
        ])
        
        silver_df = subreddit_df.mapInPandas(
            lambda iterator: process_batch(iterator, gemini_keys, ref_broadcast),
            schema=schema,
        )
        
        # Add analysis_id
        silver_df = silver_df.withColumn(
            "analysis_id",
            spark_hash(col("comment_id"), col("brand_name"), col("product_name"), col("aspect"))
            .cast(StringType()),
        )
        
        # Write immediately
        logger.info(f"Writing results for {subreddit} to {sentiment_table}")
        try:
            silver_df.sort("date_partition").writeTo(sentiment_table).append()
            logger.info(f"Successfully wrote results for {subreddit}")
            processed_count += subreddit_count
        except Exception as e:
            logger.error(f"Failed to write results for {subreddit}: {e}")
            # Continue to next subreddit instead of failing entire job
            continue
            
    # 8. Emit metrics
    end_time = datetime.now(timezone.utc)
    duration_seconds = (end_time - start_time).total_seconds()
    
    metrics_writer.add_metric("CommentsAnalyzedByGemini", float(processed_count), unit='Count')
    metrics_writer.add_metric("JobDurationSeconds", duration_seconds, unit='Seconds')
    metrics_writer.publish()
    
    logger.info(f"Silver-sentiment job completed successfully in {duration_seconds:.2f} seconds")
    logger.info(f"Analyzed {processed_count} comments across {len(subreddits)} subreddits")
    
    job.commit()

except Exception as e:
    logger.error(f"Silver-sentiment job failed: {str(e)}")
    raise
