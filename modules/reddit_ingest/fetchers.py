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
    )
except Exception:  # pragma: no cover - defensive import
    RequestException = ResponseException = ServerError = Forbidden = RateLimitExceeded = Exception


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    base_sleep: float = 1.0  # seconds
    max_sleep: float = 30.0

    def backoff(self, attempt: int) -> float:
        sleep = min(self.max_sleep, self.base_sleep * (2 ** (attempt - 1)))
        return sleep


def execute_with_retry(fn, *, policy: RetryPolicy):
    attempt = 1
    while True:
        try:
            return fn()
        except (RequestException, ResponseException, ServerError, Forbidden, RateLimitExceeded, RedditAPIException) as e:
            if attempt >= policy.max_attempts:
                raise
            time.sleep(policy.backoff(attempt))
            attempt += 1


class SubmissionFetcher:
    """Fetch submissions from a subreddit in recent or time-window mode."""

    def __init__(self, reddit: praw.Reddit, *, retry_policy: Optional[RetryPolicy] = None):
        self.reddit = reddit
        self.retry_policy = retry_policy or RetryPolicy()

    def _listing_new(self, subreddit_name: str, limit: Optional[int]) -> Iterable[Submission]:
        subreddit = execute_with_retry(
            lambda: self.reddit.subreddit(subreddit_name), policy=self.retry_policy
        )
        return subreddit.new(limit=limit)

    def iterate_recent(
        self,
        subreddit_name: str,
        *,
        limit: Optional[int],
        sleep_ms: int = 250,
    ) -> Generator[Submission, None, None]:
        for s in self._listing_new(subreddit_name, limit=limit):
            yield s
            if sleep_ms:
                time.sleep(max(0.0, float(sleep_ms) / 1000.0))

    def iterate_window(
        self,
        subreddit_name: str,
        *,
        start_ts: int,
        end_ts: Optional[int],
        sleep_ms: int = 250,
    ) -> Generator[Submission, None, None]:
        end_ts = end_ts or int(time.time())
        for s in self._listing_new(subreddit_name, limit=None):
            created = int(getattr(s, "created_utc", 0) or 0)
            if created < start_ts:
                break
            if created <= end_ts:
                yield s
                if sleep_ms:
                    time.sleep(max(0.0, float(sleep_ms) / 1000.0))


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
        # Expand MoreComments with retry
        execute_with_retry(
            lambda: submission.comments.replace_more(limit=None),
            policy=self.retry_policy,
        )
        count = 0
        for c in submission.comments.list():
            yield c
            count += 1
            if max_comments is not None and count >= max_comments:
                break


