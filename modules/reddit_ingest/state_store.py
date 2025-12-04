from __future__ import annotations

"""
State management for Reddit ingestion watermarks.

Encapsulates reading and writing the last_run_ts watermark per subreddit
to AWS Systems Manager Parameter Store.
"""

import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger(__name__)


class RedditIngestionStateStore:
    """
    Manage per-subreddit ingestion state in SSM Parameter Store.

    Parameters are stored under:
      {base_path}/{environment}/{subreddit}/last_run_ts
    """

    PARAMETER_NAME_SUFFIX = "last_run_ts"

    def __init__(self, *, base_path: str, environment: str) -> None:
        self._ssm = boto3.client("ssm")
        self._base_path = base_path.rstrip("/")
        self._environment = environment

    def _parameter_name(self, subreddit: str) -> str:
        clean_subreddit = subreddit.strip()
        return f"{self._base_path}/{self._environment}/{clean_subreddit}/{self.PARAMETER_NAME_SUFFIX}"

    def load_last_run_timestamp(self, subreddit: str) -> Optional[int]:
        """
        Load last_run_ts for a subreddit.

        Returns None if the parameter does not exist (first run).
        """
        name = self._parameter_name(subreddit)
        try:
            response = self._ssm.get_parameter(Name=name)
        except self._ssm.exceptions.ParameterNotFound:
            return None
        except ClientError as exc:
            LOGGER.error("Failed to read ingestion state from SSM: %s", exc)
            raise

        value = response.get("Parameter", {}).get("Value")
        if value is None:
            return None

        try:
            return int(value)
        except ValueError:
            LOGGER.warning("Invalid last_run_ts '%s' in parameter %s", value, name)
            return None

    def save_last_run_timestamp(self, subreddit: str, timestamp: int) -> None:
        """
        Persist last_run_ts for a subreddit.
        """
        name = self._parameter_name(subreddit)
        try:
            self._ssm.put_parameter(
                Name=name,
                Value=str(int(timestamp)),
                Type="String",
                Overwrite=True,
            )
        except ClientError as exc:
            LOGGER.error("Failed to write ingestion state to SSM: %s", exc)
            raise


