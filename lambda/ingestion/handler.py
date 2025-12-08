"""
AWS Lambda handler for Reddit listing ingestion.

This handler fetches up to 1000 submissions from Reddit's /top or /controversial
endpoints and writes them to S3. Each invocation handles a single listing type.
Step Functions orchestrates separate calls for top and controversial.

Event structure:
{
    "subreddit": "wallstreetbets",
    "sort_type": "top",           # "top" or "controversial"
    "time_filter": "all",         # "all", "year", "month", "week", "day", "hour"
    "max_items": 1000,            # Max 1000 per Reddit API
    "include_comments": true
}
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from modules.reddit_ingest.reddit_client import RedditClientFactory
from modules.reddit_ingest.fetchers import CommentFetcher
from modules.reddit_ingest.simple_listing_fetcher import SimpleListingFetcher
from modules.reddit_ingest.s3_storage import S3JsonlWriter
from modules.reddit_ingest.ingestion_service import RedditIngestionService
from modules.reddit_ingest.state_store import RedditCheckpointStore, CheckpointRecord
from modules.reddit_ingest.run_metadata import RedditIngestionRunRecorder


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
LOGGER = logging.getLogger(__name__)


ENV_LANDING_BUCKET_NAME = "LANDING_BUCKET_NAME"
ENV_LANDING_BUCKET_PREFIX = "LANDING_BUCKET_PREFIX"
ENV_REDDIT_SECRET_ARN = "REDDIT_SECRET_ARN"
ENV_DEFAULT_SUBREDDIT = "DEFAULT_SUBREDDIT"
ENV_ENVIRONMENT = "ENVIRONMENT"
ENV_RUNS_TABLE_NAME = "RUNS_TABLE_NAME"
ENV_METRICS_NAMESPACE = "METRICS_NAMESPACE"
ENV_CHECKPOINTS_TABLE_NAME = "CHECKPOINTS_TABLE_NAME"

REDDIT_CLIENT_ID_KEY = "REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET_KEY = "REDDIT_CLIENT_SECRET"
REDDIT_USER_AGENT_KEY = "REDDIT_USER_AGENT"

DEFAULT_TIME_FILTER = "all"
DEFAULT_MAX_ITEMS = 1000
SUPPORTED_SORT_TYPES = ("top", "controversial")


def _load_reddit_credentials(secret_arn: str) -> Dict[str, str]:
    """
    Load Reddit API credentials from AWS Secrets Manager.
    
    Returns:
        Dict with Reddit credentials.

    Raises:
        RuntimeError: If secret cannot be loaded or is invalid.
    """
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        raise RuntimeError(f"Failed to load Reddit secret from {secret_arn}") from exc

    secret_string = response.get("SecretString") or ""
    try:
        payload = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Reddit secret is not valid JSON") from exc

    for key in (REDDIT_CLIENT_ID_KEY, REDDIT_CLIENT_SECRET_KEY, REDDIT_USER_AGENT_KEY):
        if not payload.get(key):
            raise RuntimeError(f"Missing '{key}' in Reddit secret payload")
    
    return payload


def _build_s3_prefix(base_prefix: str, sort_type: str, subreddit: str) -> str:
    """
    Build S3 prefix with date partitioning.

    Format: reddit/{sort_type}/subreddit={subreddit}/date=DD-MM-YYYY/
    """
    clean_prefix = (base_prefix or "").rstrip("/")
    now = datetime.now(timezone.utc)
    date_str = f"{now.day:02d}-{now.month:02d}-{now.year}"
    return (
        f"{clean_prefix}/{sort_type}/"
        f"subreddit={subreddit}/"
        f"date={date_str}/"
    )


def _utc_now_timestamp() -> int:
    """Return current UTC timestamp as integer."""
    return int(datetime.now(timezone.utc).timestamp())


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Reddit listing ingestion.

    Fetches submissions from a single Reddit listing endpoint (top or controversial)
    and writes them to S3. Each invocation handles one sort_type.
    """
    landing_bucket = os.environ.get(ENV_LANDING_BUCKET_NAME)
    if not landing_bucket:
        raise RuntimeError(f"Missing required environment variable {ENV_LANDING_BUCKET_NAME}")

    secret_arn = os.environ.get(ENV_REDDIT_SECRET_ARN)
    if not secret_arn:
        raise RuntimeError(f"Missing required environment variable {ENV_REDDIT_SECRET_ARN}")

    base_prefix = os.environ.get(ENV_LANDING_BUCKET_PREFIX, "reddit")
    environment = os.environ.get(ENV_ENVIRONMENT, "prod")
    runs_table_name = os.environ.get(ENV_RUNS_TABLE_NAME)
    metrics_namespace = os.environ.get(ENV_METRICS_NAMESPACE, "RedditIngestion")
    checkpoints_table_name = os.environ.get(ENV_CHECKPOINTS_TABLE_NAME)

    if not runs_table_name:
        raise RuntimeError(f"Missing required environment variable {ENV_RUNS_TABLE_NAME}")
    if not checkpoints_table_name:
        raise RuntimeError(f"Missing required environment variable {ENV_CHECKPOINTS_TABLE_NAME}")

    event = event or {}
    subreddit = str(event.get("subreddit") or os.getenv(ENV_DEFAULT_SUBREDDIT, "all"))
    sort_type = str(event.get("sort_type", "top")).lower()
    time_filter = str(event.get("time_filter", DEFAULT_TIME_FILTER)).lower()
    max_items = int(event.get("max_items", DEFAULT_MAX_ITEMS))
    include_comments = bool(event.get("include_comments", True))

    if sort_type not in SUPPORTED_SORT_TYPES:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": f"Invalid sort_type: {sort_type}. Must be one of {SUPPORTED_SORT_TYPES}"
            }),
        }

    LOGGER.info(
        "Starting listing ingestion: subreddit=%s sort_type=%s time_filter=%s max_items=%d",
        subreddit,
        sort_type,
        time_filter,
        max_items,
    )

    credentials = _load_reddit_credentials(secret_arn)
    for key, value in credentials.items():
        os.environ[key] = str(value)

    run_timestamp = _utc_now_timestamp()
    s3_prefix = _build_s3_prefix(base_prefix, sort_type, subreddit)

    reddit_client = RedditClientFactory.create()
    simple_fetcher = SimpleListingFetcher(reddit_client)
    comment_fetcher = CommentFetcher()
    
    submissions_writer = S3JsonlWriter(
        bucket_name=landing_bucket,
        base_prefix=s3_prefix,
        filename="submissions.jsonl",
    )
    comments_writer = S3JsonlWriter(
        bucket_name=landing_bucket,
        base_prefix=s3_prefix,
        filename="comments.jsonl",
    )
    
    service = RedditIngestionService(
        comment_fetcher=comment_fetcher,
        submissions_writer=submissions_writer,
        comments_writer=comments_writer,
        s3_prefix=s3_prefix,
        run_timestamp=run_timestamp,
    )
    
    result = service.ingest_listing(
        subreddit_name=subreddit,
        simple_fetcher=simple_fetcher,
        sort_type=sort_type,
        time_filter=time_filter,
        max_items=max_items,
        include_comments=include_comments,
    )
    
    checkpoint_store = RedditCheckpointStore(
        table_name=checkpoints_table_name,
        environment=environment,
    )
    checkpoint_store.save_checkpoint(CheckpointRecord(
        subreddit=subreddit,
        checkpoint_type="listing",
        last_timestamp=run_timestamp,
        sort_type=sort_type,
        time_filter=time_filter,
        items_fetched=result.submissions_count,
        s3_location=f"s3://{landing_bucket}/{s3_prefix}",
    ))
    
    run_recorder = RedditIngestionRunRecorder(
        dynamodb_table_name=runs_table_name,
        cloudwatch_namespace=metrics_namespace,
        environment=environment,
    )
    run_recorder.record_run(
        result=result,
        mode=f"listing_{sort_type}",
        subreddit=subreddit,
        run_timestamp=run_timestamp,
    )
    
    LOGGER.info(
        "Completed listing ingestion: subreddit=%s sort_type=%s submissions=%d comments=%d",
        subreddit,
        sort_type,
        result.submissions_count,
        result.comments_count,
    )
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "subreddit": subreddit,
            "sort_type": sort_type,
            "time_filter": time_filter,
            "submissions_count": result.submissions_count,
            "comments_count": result.comments_count,
            "s3_location": f"s3://{landing_bucket}/{s3_prefix}",
        }),
    }
