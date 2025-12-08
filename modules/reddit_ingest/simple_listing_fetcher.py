"""
Simple listing-based fetcher for Reddit top and controversial endpoints.

This module provides a straightforward fetcher that retrieves up to 1000
submissions from Reddit's /top or /controversial listings. PRAW handles
pagination automatically. No deduplication or filtering is applied -
downstream ETL handles data quality.
"""

from __future__ import annotations

import logging
import time
from typing import Generator, Optional

import praw
from praw.models import Submission

from .fetchers import RetryPolicy, execute_with_retry


LOGGER = logging.getLogger(__name__)

SUPPORTED_SORT_TYPES = ("top", "controversial")
DEFAULT_TIME_FILTER = "all"
DEFAULT_LIMIT = 500
DEFAULT_SLEEP_MS = 20  # PRAW handles rate limiting internally


class SimpleListingFetcher:
    """
    Fetch submissions from Reddit /top or /controversial listings.

    Each call to fetch_listing retrieves up to 1000 submissions from a single
    listing endpoint. PRAW handles pagination automatically behind the scenes.
    No deduplication is performed - downstream Glue ETL handles that.
    """

    def __init__(
        self,
        reddit: praw.Reddit,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self.reddit = reddit
        self.retry_policy = retry_policy or RetryPolicy()

    def fetch_listing(
        self,
        subreddit_name: str,
        sort_type: str,
        time_filter: str = DEFAULT_TIME_FILTER,
        limit: int = DEFAULT_LIMIT,
        sleep_ms: int = DEFAULT_SLEEP_MS,
    ) -> Generator[Submission, None, None]:
        """
        Fetch submissions from a Reddit listing endpoint.

        Args:
            subreddit_name: Name of the subreddit to fetch from.
            sort_type: Either "top" or "controversial".
            time_filter: Time filter for the listing ("all", "year", "month", "week", "day", "hour").
            limit: Maximum number of submissions to fetch (max 1000 per Reddit API).
            sleep_ms: Milliseconds to sleep after every 100 items for rate limiting.

        Yields:
            Submission objects from the listing.

        Raises:
            ValueError: If sort_type is not "top" or "controversial".
        """
        sort_type_lower = sort_type.lower()
        if sort_type_lower not in SUPPORTED_SORT_TYPES:
            raise ValueError(
                f"Unsupported sort_type: {sort_type}. Must be one of {SUPPORTED_SORT_TYPES}"
            )

        LOGGER.info(
            "Fetching %s (time_filter=%s, limit=%d) for subreddit=%s",
            sort_type_lower,
            time_filter,
            limit,
            subreddit_name,
        )

        subreddit = execute_with_retry(
            lambda: self.reddit.subreddit(subreddit_name),
            policy=self.retry_policy,
        )

        if sort_type_lower == "top":
            listing = subreddit.top(time_filter=time_filter, limit=limit)
        else:
            listing = subreddit.controversial(time_filter=time_filter, limit=limit)

        count = 0
        for submission in listing:
            yield submission
            count += 1

            if sleep_ms and count % 100 == 0:
                time.sleep(float(sleep_ms) / 1000.0)

        LOGGER.info(
            "Completed fetching %s for subreddit=%s, total_items=%d",
            sort_type_lower,
            subreddit_name,
            count,
        )


