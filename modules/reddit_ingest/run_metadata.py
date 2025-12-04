from __future__ import annotations

"""
Run metadata and metrics recording for Reddit ingestion.

This module is responsible for persisting per-window ingestion runs to
Amazon DynamoDB and publishing summary metrics to Amazon CloudWatch.
"""

import logging
import time
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from .ingestion_service import RedditIngestionResult


LOGGER = logging.getLogger(__name__)


class RedditIngestionRunRecorder:
    """
    Record ingestion run metadata and metrics for observability.
    """

    def __init__(
        self,
        *,
        dynamodb_table_name: str,
        cloudwatch_namespace: str,
        environment: str,
    ) -> None:
        self._dynamodb_table_name = dynamodb_table_name
        self._cloudwatch_namespace = cloudwatch_namespace
        self._environment = environment
        self._dynamodb = boto3.client("dynamodb")
        self._cloudwatch = boto3.client("cloudwatch")

    def record_run(
        self,
        result: RedditIngestionResult,
        *,
        mode: str,
        subreddit: str,
    ) -> None:
        """
        Persist run metadata to DynamoDB and publish CloudWatch metrics.
        """
        ingested_at_ts = int(time.time())
        pk = f"ENV#{self._environment}#SUBREDDIT#{subreddit}"
        sk = f"RUN#{result.window_start_ts or ingested_at_ts}"

        self._put_run_item(
            pk=pk,
            sk=sk,
            result=result,
            mode=mode,
            subreddit=subreddit,
            ingested_at_ts=ingested_at_ts,
        )
        self._publish_metrics(result=result, subreddit=subreddit)

    def _put_run_item(
        self,
        *,
        pk: str,
        sk: str,
        result: RedditIngestionResult,
        mode: str,
        subreddit: str,
        ingested_at_ts: int,
    ) -> None:
        item: Dict[str, Dict[str, str]] = {
            "pk": {"S": pk},
            "sk": {"S": sk},
            "Environment": {"S": self._environment},
            "Subreddit": {"S": subreddit},
            "Mode": {"S": mode},
            "SubmissionsCount": {"N": str(result.submissions_count)},
            "CommentsCount": {"N": str(result.comments_count)},
            "IngestedAtTs": {"N": str(ingested_at_ts)},
        }

        if result.window_start_ts:
            item["WindowStartTs"] = {"N": str(result.window_start_ts)}
        if result.window_end_ts:
            item["WindowEndTs"] = {"N": str(result.window_end_ts)}

        try:
            self._dynamodb.put_item(
                TableName=self._dynamodb_table_name,
                Item=item,
            )
            LOGGER.info(
                "Recorded ingestion run pk=%s sk=%s submissions=%d comments=%d",
                pk,
                sk,
                result.submissions_count,
                result.comments_count,
            )
        except ClientError as exc:
            LOGGER.error("Failed to write ingestion run to DynamoDB: %s", exc)
            raise

    def _publish_metrics(self, *, result: RedditIngestionResult, subreddit: str) -> None:
        try:
            self._cloudwatch.put_metric_data(
                Namespace=self._cloudwatch_namespace,
                MetricData=[
                    {
                        "MetricName": "NewSubmissions",
                        "Dimensions": [
                            {"Name": "Environment", "Value": self._environment},
                            {"Name": "Subreddit", "Value": subreddit},
                        ],
                        "Value": float(result.submissions_count),
                        "Unit": "Count",
                    },
                    {
                        "MetricName": "NewComments",
                        "Dimensions": [
                            {"Name": "Environment", "Value": self._environment},
                            {"Name": "Subreddit", "Value": subreddit},
                        ],
                        "Value": float(result.comments_count),
                        "Unit": "Count",
                    },
                ],
            )
            LOGGER.info(
                "Published metrics for subreddit=%s submissions=%d comments=%d",
                subreddit,
                result.submissions_count,
                result.comments_count,
            )
        except ClientError as exc:
            LOGGER.error("Failed to publish ingestion metrics: %s", exc)
            raise


