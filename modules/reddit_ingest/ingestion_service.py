from __future__ import annotations

"""
High-level orchestration for Reddit ingestion.

This module wires together the low-level fetchers, serializers, and storage
writers so that callers (for example, an AWS Lambda handler) can trigger a
complete ingestion run with a simple method call.
"""

from dataclasses import dataclass
import logging
from typing import Optional, Dict, Any

import praw
from praw.models import Submission

from .fetchers import SubmissionFetcher, CommentFetcher
from .serializers import SubmissionSerializer, CommentSerializer


LOGGER = logging.getLogger(__name__)

@dataclass
class RedditIngestionResult:
    """
    Summary of a single ingestion run.
    """

    subreddit: str
    submissions_count: int
    comments_count: int
    s3_prefix: str
    window_start_ts: int = 0
    window_end_ts: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "subreddit": self.subreddit,
            "submissions_count": self.submissions_count,
            "comments_count": self.comments_count,
            "s3_prefix": self.s3_prefix,
            "window_start_ts": self.window_start_ts,
            "window_end_ts": self.window_end_ts,
        }


class RedditIngestionService:
    """
    Coordinate fetching submissions and comments and writing them to storage.

    Storage is provided by two writer-like objects for submissions and comments.
    These writers are expected to expose `write`, `flush`, and `close` methods,
    such as JsonlWriter or S3JsonlWriter.
    """

    def __init__(
        self,
        reddit_client: praw.Reddit,
        submission_fetcher: SubmissionFetcher,
        comment_fetcher: CommentFetcher,
        submissions_writer,
        comments_writer,
        s3_prefix: str,
    ) -> None:
        self._reddit_client = reddit_client
        self._submission_fetcher = submission_fetcher
        self._comment_fetcher = comment_fetcher
        self._submissions_writer = submissions_writer
        self._comments_writer = comments_writer
        self._s3_prefix = s3_prefix

    def ingest_recent(
        self,
        subreddit_name: str,
        *,
        limit: Optional[int] = None,
        include_comments: bool = True,
    ) -> RedditIngestionResult:
        """
        Ingest recent submissions (and optionally comments) for a subreddit.
        """
        LOGGER.info("Starting recent ingestion for subreddit=%s", subreddit_name)
        submissions_count = 0
        comments_count = 0

        for submission in self._submission_fetcher.iterate_recent(
            subreddit_name=subreddit_name,
            limit=limit,
        ):
            submissions_count += 1
            self._write_submission(submission)

            if include_comments:
                comments_count += self._write_comments_for_submission(submission)

        self._submissions_writer.flush()
        self._comments_writer.flush()

        result = RedditIngestionResult(
            subreddit=subreddit_name,
            submissions_count=submissions_count,
            comments_count=comments_count,
            s3_prefix=self._s3_prefix,
        )
        LOGGER.info(
            "Completed recent ingestion for subreddit=%s submissions=%d comments=%d",
            subreddit_name,
            submissions_count,
            comments_count,
        )
        return result

    def ingest_window(
        self,
        subreddit_name: str,
        *,
        start_ts: int,
        end_ts: int,
        include_comments: bool = True,
    ) -> RedditIngestionResult:
        """
        Ingest submissions created within a time window [start_ts, end_ts].

        This is used for incremental ingestion based on created_utc timestamps.
        """
        LOGGER.info(
            "Starting window ingestion for subreddit=%s start_ts=%d end_ts=%d",
            subreddit_name,
            start_ts,
            end_ts,
        )
        submissions_count = 0
        comments_count = 0

        for submission in self._submission_fetcher.iterate_window(
            subreddit_name=subreddit_name,
            start_ts=start_ts,
            end_ts=end_ts,
        ):
            submissions_count += 1
            self._write_submission(submission)

            if include_comments:
                comments_count += self._write_comments_for_submission(submission)

        self._submissions_writer.flush()
        self._comments_writer.flush()

        result = RedditIngestionResult(
            subreddit=subreddit_name,
            submissions_count=submissions_count,
            comments_count=comments_count,
            s3_prefix=self._s3_prefix,
            window_start_ts=start_ts,
            window_end_ts=end_ts,
        )
        LOGGER.info(
            "Completed window ingestion for subreddit=%s start_ts=%d end_ts=%d submissions=%d comments=%d",
            subreddit_name,
            start_ts,
            end_ts,
            submissions_count,
            comments_count,
        )
        return result

    def _write_submission(self, submission: Submission) -> None:
        serialized = SubmissionSerializer.to_dict(submission)
        self._submissions_writer.write(serialized)

    def _write_comments_for_submission(self, submission: Submission) -> int:
        written = 0
        for comment in self._comment_fetcher.iterate_comments(submission=submission):
            serialized = CommentSerializer.to_dict(comment)
            self._comments_writer.write(serialized)
            written += 1
        return written


