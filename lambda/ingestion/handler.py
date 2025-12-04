from __future__ import annotations

"""
AWS Lambda handler for Reddit ingestion (hourly, incremental, window-based).

This handler:
- Reads configuration from environment variables and the incoming event.
- Loads Reddit API credentials from AWS Secrets Manager.
- Resolves one or more hourly ingestion windows using SSM-based watermarks.
- Uses the reddit_ingest package to fetch submissions and comments per window.
- Records run metadata and metrics per window.
- Writes JSON Lines files to the landing zone S3 bucket.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

from modules.reddit_ingest.reddit_client import RedditClientFactory
from modules.reddit_ingest.fetchers import SubmissionFetcher, CommentFetcher
from modules.reddit_ingest.s3_storage import S3JsonlWriter
from modules.reddit_ingest.ingestion_service import RedditIngestionService
from modules.reddit_ingest.state_store import RedditIngestionStateStore
from modules.reddit_ingest.run_metadata import RedditIngestionRunRecorder


ENV_LANDING_BUCKET_NAME = "LANDING_BUCKET_NAME"
ENV_LANDING_BUCKET_PREFIX = "LANDING_BUCKET_PREFIX"
ENV_REDDIT_SECRET_ARN = "REDDIT_SECRET_ARN"
ENV_DEFAULT_SUBREDDIT = "DEFAULT_SUBREDDIT"
ENV_MAX_SUBMISSIONS = "MAX_SUBMISSIONS"
ENV_ENVIRONMENT = "ENVIRONMENT"
ENV_STATE_BASE_PATH = "STATE_BASE_PATH"
ENV_RUNS_TABLE_NAME = "RUNS_TABLE_NAME"
ENV_METRICS_NAMESPACE = "METRICS_NAMESPACE"
ENV_MAX_HOURLY_WINDOWS = "MAX_HOURLY_WINDOWS_PER_RUN"

REDDIT_CLIENT_ID_KEY = "REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET_KEY = "REDDIT_CLIENT_SECRET"
REDDIT_USER_AGENT_KEY = "REDDIT_USER_AGENT"

HOUR_SECONDS = 3600


LOGGER = logging.getLogger(__name__)


def _load_reddit_credentials(secret_arn: str) -> None:
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
        value = payload.get(key)
        if not value:
            raise RuntimeError(f"Missing '{key}' in Reddit secret payload")
        os.environ[key] = str(value)


def _resolve_subreddit(event: Dict[str, Any]) -> str:
    if isinstance(event, dict) and "subreddit" in event:
        return str(event["subreddit"])
    return os.getenv(ENV_DEFAULT_SUBREDDIT, "all")


def _resolve_max_submissions(event: Dict[str, Any]) -> int:
    if isinstance(event, dict) and "max_submissions" in event:
        try:
            return int(event["max_submissions"])
        except (TypeError, ValueError):
            pass
    try:
        return int(os.getenv(ENV_MAX_SUBMISSIONS, "100"))
    except ValueError:
        return 100


def _build_s3_prefix(base_prefix: str, subreddit: str) -> str:
    clean_prefix = (base_prefix or "").rstrip("/")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d/%H%M%S")
    return f"{clean_prefix}/{subreddit}/{timestamp}/"


def _utc_now_timestamp() -> int:
    return int(datetime.utcnow().timestamp())


def _is_backfill_mode(event: Dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False
    if event.get("mode") == "backfill":
        return True
    return "start_ts" in event and "end_ts" in event


def _align_to_hour(ts: int) -> int:
    """
    Floor a timestamp to the previous hour boundary.
    """
    return ts - (ts % HOUR_SECONDS)


def _build_hourly_windows(
    start_ts: int,
    end_ts: int,
    *,
    max_hours: int | None,
) -> List[Tuple[int, int]]:
    """
    Split an interval into hourly windows [start, end).
    """
    if start_ts >= end_ts:
        return []

    windows: List[Tuple[int, int]] = []
    current = start_ts
    while current < end_ts and (max_hours is None or len(windows) < max_hours):
        window_end = min(current + HOUR_SECONDS, end_ts)
        windows.append((current, window_end))
        current = window_end
    return windows


def _resolve_hourly_windows(
    event: Dict[str, Any],
    subreddit: str,
    state_store: RedditIngestionStateStore,
    max_hours_per_run: int,
) -> Tuple[List[Tuple[int, int]], bool]:
    """
    Determine hourly windows to process and whether to update state.
    """
    now_ts = _utc_now_timestamp()
    end_aligned = _align_to_hour(now_ts)

    if _is_backfill_mode(event):
        start_ts = int(event["start_ts"])
        end_ts = int(event["end_ts"])
        start_ts_aligned = _align_to_hour(start_ts)
        windows = _build_hourly_windows(
            start_ts=start_ts_aligned,
            end_ts=end_ts,
            max_hours=None,
        )
        LOGGER.info(
            "Backfill mode for subreddit=%s windows=%s",
            subreddit,
            windows,
        )
        return windows, False

    last_run_ts = state_store.load_last_run_timestamp(subreddit=subreddit)

    if last_run_ts is None:
        # First run: process the previous full hour.
        if end_aligned < HOUR_SECONDS:
            return [], False
        start_ts = end_aligned - HOUR_SECONDS
        LOGGER.info(
            "First incremental run for subreddit=%s window=(%d,%d)",
            subreddit,
            start_ts,
            end_aligned,
        )
        return [(start_ts, end_aligned)], True

    # Subsequent runs: process from last_processed_hour_end_ts up to now (capped).
    if last_run_ts >= end_aligned:
        LOGGER.info(
            "No new full hours to process for subreddit=%s last_run_ts=%d end_aligned=%d",
            subreddit,
            last_run_ts,
            end_aligned,
        )
        return [], True

    windows = _build_hourly_windows(
        start_ts=last_run_ts,
        end_ts=end_aligned,
        max_hours=max_hours_per_run,
    )
    LOGGER.info(
        "Incremental run for subreddit=%s last_run_ts=%d end_aligned=%d windows=%s",
        subreddit,
        last_run_ts,
        end_aligned,
        windows,
    )
    return windows, True


def _resolve_max_hourly_windows() -> int:
    try:
        value = int(os.getenv(ENV_MAX_HOURLY_WINDOWS, "1"))
        return max(1, value)
    except ValueError:
        return 1


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    landing_bucket = os.environ.get(ENV_LANDING_BUCKET_NAME)
    if not landing_bucket:
        raise RuntimeError(f"Missing required environment variable {ENV_LANDING_BUCKET_NAME}")

    secret_arn = os.environ.get(ENV_REDDIT_SECRET_ARN)
    if not secret_arn:
        raise RuntimeError(f"Missing required environment variable {ENV_REDDIT_SECRET_ARN}")

    base_prefix = os.environ.get(ENV_LANDING_BUCKET_PREFIX, "reddit/")
    environment = os.environ.get(ENV_ENVIRONMENT, "prod")
    state_base_path = os.environ.get(ENV_STATE_BASE_PATH, "/reddit-ingest")
    runs_table_name = os.environ.get(ENV_RUNS_TABLE_NAME)
    metrics_namespace = os.environ.get(ENV_METRICS_NAMESPACE, "RedditIngestion")

    if not runs_table_name:
        raise RuntimeError(f"Missing required environment variable {ENV_RUNS_TABLE_NAME}")

    subreddit = _resolve_subreddit(event or {})
    max_submissions = _resolve_max_submissions(event or {})
    max_hours_per_run = _resolve_max_hourly_windows()

    LOGGER.info(
        "Handling event for subreddit=%s environment=%s max_submissions=%d max_hours_per_run=%d",
        subreddit,
        environment,
        max_submissions,
        max_hours_per_run,
    )

    _load_reddit_credentials(secret_arn)

    reddit_client = RedditClientFactory.create()
    submission_fetcher = SubmissionFetcher(reddit_client)
    comment_fetcher = CommentFetcher()

    state_store = RedditIngestionStateStore(
        base_path=state_base_path,
        environment=environment,
    )

    windows, should_update_state = _resolve_hourly_windows(
        event=event or {},
        subreddit=subreddit,
        state_store=state_store,
        max_hours_per_run=max_hours_per_run,
    )

    if not windows:
        LOGGER.info("No windows to process for subreddit=%s", subreddit)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No windows to process", "subreddit": subreddit}),
        }

    run_recorder = RedditIngestionRunRecorder(
        dynamodb_table_name=runs_table_name,
        cloudwatch_namespace=metrics_namespace,
        environment=environment,
    )

    total_submissions = 0
    total_comments = 0

    for start_ts, end_ts in windows:
        s3_prefix = _build_s3_prefix(base_prefix, subreddit)

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
            reddit_client=reddit_client,
            submission_fetcher=submission_fetcher,
            comment_fetcher=comment_fetcher,
            submissions_writer=submissions_writer,
            comments_writer=comments_writer,
            s3_prefix=s3_prefix,
        )

        mode = "backfill" if _is_backfill_mode(event or {}) else "incremental"

        LOGGER.info(
            "Processing window for subreddit=%s start_ts=%d end_ts=%d mode=%s",
            subreddit,
            start_ts,
            end_ts,
            mode,
        )

        result = service.ingest_window(
            subreddit_name=subreddit,
            start_ts=start_ts,
            end_ts=end_ts,
            include_comments=True,
        )

        run_recorder.record_run(
            result=result,
            mode=mode,
            subreddit=subreddit,
        )

        total_submissions += result.submissions_count
        total_comments += result.comments_count

        if should_update_state:
            state_store.save_last_run_timestamp(subreddit=subreddit, timestamp=end_ts)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "subreddit": subreddit,
                "windows_processed": windows,
                "total_submissions": total_submissions,
                "total_comments": total_comments,
            }
        ),
    }


