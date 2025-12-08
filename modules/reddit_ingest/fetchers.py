"""
Reddit API fetchers for submissions and comments.

This module provides low-level fetchers for Reddit data using PRAW.
For listing-based ingestion (top/controversial), use SimpleListingFetcher instead.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generator, Iterable, Optional

import praw
from praw.exceptions import RedditAPIException
from praw.models import Submission

try:
    from prawcore.exceptions import (
        RequestException,
        ResponseException,
        ServerError,
        Forbidden,
        RateLimitExceeded,
        TooManyRequests,
    )
except Exception:  # pragma: no cover - defensive import
    RequestException = ResponseException = ServerError = Forbidden = RateLimitExceeded = TooManyRequests = Exception


@dataclass
class RetryPolicy:
    """
    Retry configuration for Reddit API calls.
    """

    max_attempts: int = 5
    base_sleep: float = 1.0  # seconds
    max_sleep: float = 30.0

    def backoff(self, attempt: int) -> float:
        """
        Compute exponential backoff sleep time for the given attempt.
        """
        sleep = min(self.max_sleep, self.base_sleep * (2 ** (attempt - 1)))
        return sleep


def execute_with_retry(fn, *, policy: RetryPolicy):
    """
    Execute the given callable with retry on transient Reddit / network errors.
    """
    attempt = 1
    while True:
        try:
            return fn()
        except (
            RequestException,
            ResponseException,
            ServerError,
            Forbidden,
            RateLimitExceeded,
            TooManyRequests,
            RedditAPIException,
        ):
            if attempt >= policy.max_attempts:
                raise
            time.sleep(policy.backoff(attempt))
            attempt += 1


class CommentFetcher:
    """Fetch and flatten all comments for a submission."""

    def __init__(self, *, retry_policy: Optional[RetryPolicy] = None):
        self.retry_policy = retry_policy or RetryPolicy()

    def iterate_comments(
        self,
        submission: Submission,
        *,
        max_comments: Optional[int] = None,
    ) -> Generator:
        """
        Iterate over all comments for a submission.

        Expands "MoreComments" objects to retrieve the full comment tree.

        Args:
            submission: The submission to fetch comments for.
            max_comments: Maximum number of comments to return (None for all).

        Yields:
            Comment objects from the submission.
        """
        execute_with_retry(
            lambda: submission.comments.replace_more(limit=None),
            policy=self.retry_policy,
        )
        count = 0
        for comment in submission.comments.list():
            yield comment
            count += 1
            if max_comments is not None and count >= max_comments:
                break
