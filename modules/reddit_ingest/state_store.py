"""
State management for Reddit ingestion.

This module provides a DynamoDB-backed checkpoint store for tracking
ingestion state per subreddit and listing type.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger(__name__)


@dataclass
class CheckpointRecord:
    """
    Represent a checkpoint for a subreddit and checkpoint type.

    For listing-based ingestion, sort_type and time_filter identify the
    specific Reddit listing endpoint that was fetched.
    """

    subreddit: str
    checkpoint_type: str
    last_token: Optional[str] = None
    last_timestamp: Optional[int] = None
    sort_type: Optional[str] = None
    time_filter: Optional[str] = None
    items_fetched: Optional[int] = None
    s3_location: Optional[str] = None


class RedditCheckpointStore:
    """
    Manage ingestion checkpoints in DynamoDB.

    This store is designed for listing-based ingestion where we track
    the last run timestamp, items fetched, and S3 location for each
    (subreddit, sort_type, time_filter) combination.
    """

    def __init__(self, *, table_name: str, environment: str) -> None:
        if not table_name:
            raise ValueError("table_name must be provided")
        self._table_name = table_name
        self._environment = environment
        self._client = boto3.client("dynamodb")

    def _build_pk(self, subreddit: str) -> str:
        """Build consistent partition key for a subreddit."""
        return f"ENV#{self._environment}#SUBREDDIT#{subreddit.strip()}"

    def _build_sk(
        self,
        checkpoint_type: str,
        sort_type: Optional[str] = None,
        time_filter: Optional[str] = None,
    ) -> str:
        """
        Build consistent sort key for a checkpoint type.

        For listing checkpoints, includes sort_type and time_filter to
        uniquely identify the Reddit listing endpoint.
        """
        if sort_type and time_filter:
            return f"CHECKPOINT#{checkpoint_type}#{sort_type}#{time_filter}"
        return f"CHECKPOINT#{checkpoint_type}"

    def load_checkpoint(
        self,
        *,
        subreddit: str,
        checkpoint_type: str,
        sort_type: Optional[str] = None,
        time_filter: Optional[str] = None,
    ) -> Optional[CheckpointRecord]:
        """
        Load a checkpoint for the given subreddit and type.

        For listing checkpoints, sort_type and time_filter are required to
        identify the specific Reddit listing endpoint.
        """
        pk = self._build_pk(subreddit)
        sk = self._build_sk(checkpoint_type, sort_type, time_filter)
        try:
            response = self._client.get_item(
                TableName=self._table_name,
                Key={"pk": {"S": pk}, "sk": {"S": sk}},
            )
        except ClientError as exc:
            LOGGER.error("Failed to read checkpoint from DynamoDB: %s", exc)
            raise

        item = response.get("Item")
        if not item:
            return None

        last_token = item.get("LastToken", {}).get("S")
        ts_attr = item.get("LastTimestamp", {}).get("N")
        last_ts: Optional[int] = None
        if ts_attr is not None:
            try:
                last_ts = int(ts_attr)
            except ValueError:
                LOGGER.warning(
                    "Invalid LastTimestamp '%s' for pk=%s sk=%s",
                    ts_attr,
                    pk,
                    sk,
                )

        items_fetched_attr = item.get("ItemsFetched", {}).get("N")
        items_fetched: Optional[int] = None
        if items_fetched_attr is not None:
            try:
                items_fetched = int(items_fetched_attr)
            except ValueError:
                pass

        return CheckpointRecord(
            subreddit=subreddit,
            checkpoint_type=checkpoint_type,
            last_token=last_token,
            last_timestamp=last_ts,
            sort_type=item.get("SortType", {}).get("S"),
            time_filter=item.get("TimeFilter", {}).get("S"),
            items_fetched=items_fetched,
            s3_location=item.get("S3Location", {}).get("S"),
        )

    def save_checkpoint(self, record: CheckpointRecord) -> None:
        """
        Persist a checkpoint record to DynamoDB.
        """
        pk = self._build_pk(record.subreddit)
        sk = self._build_sk(record.checkpoint_type, record.sort_type, record.time_filter)

        item: Dict[str, Any] = {
            "pk": {"S": pk},
            "sk": {"S": sk},
            "Environment": {"S": self._environment},
            "Subreddit": {"S": record.subreddit},
            "CheckpointType": {"S": record.checkpoint_type},
        }

        if record.last_token is not None:
            item["LastToken"] = {"S": record.last_token}
        if record.last_timestamp is not None:
            item["LastTimestamp"] = {"N": str(int(record.last_timestamp))}
        if record.sort_type is not None:
            item["SortType"] = {"S": record.sort_type}
        if record.time_filter is not None:
            item["TimeFilter"] = {"S": record.time_filter}
        if record.items_fetched is not None:
            item["ItemsFetched"] = {"N": str(int(record.items_fetched))}
        if record.s3_location is not None:
            item["S3Location"] = {"S": record.s3_location}

        try:
            self._client.put_item(
                TableName=self._table_name,
                Item=item,
            )
        except ClientError as exc:
            LOGGER.error("Failed to write checkpoint to DynamoDB: %s", exc)
            raise
