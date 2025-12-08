"""
High-level orchestration for Reddit listing ingestion.

This module coordinates fetching submissions from Reddit's /top or /controversial
endpoints and writing them to storage. Designed for single-purpose Lambda
invocations where each call handles one listing type.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TYPE_CHECKING

from praw.models import Submission

from .fetchers import CommentFetcher
from .serializers import SubmissionSerializer, CommentSerializer

if TYPE_CHECKING:
    from .simple_listing_fetcher import SimpleListingFetcher


LOGGER = logging.getLogger(__name__)


@dataclass
class RedditIngestionResult:
    """
    Summary of a single listing ingestion run.
    """

    subreddit: str
    submissions_count: int
    comments_count: int
    s3_prefix: str
    sort_type: str = ""
    time_filter: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for JSON serialization."""
        return {
            "subreddit": self.subreddit,
            "submissions_count": self.submissions_count,
            "comments_count": self.comments_count,
            "s3_prefix": self.s3_prefix,
            "sort_type": self.sort_type,
            "time_filter": self.time_filter,
        }


class RedditIngestionService:
    """
    Coordinate fetching submissions and comments and writing them to storage.

    This service handles a single listing type (top or controversial) per
    invocation. No deduplication is performed - downstream ETL handles that.
    """

    def __init__(
        self,
        comment_fetcher: CommentFetcher,
        submissions_writer,
        comments_writer,
        s3_prefix: str,
        run_timestamp: int,
    ) -> None:
        """
        Initialize the ingestion service.

        Args:
            comment_fetcher: CommentFetcher instance for fetching comments.
            submissions_writer: Writer for submissions (S3JsonlWriter).
            comments_writer: Writer for comments (S3JsonlWriter).
            s3_prefix: S3 prefix where data will be written.
            run_timestamp: Unix timestamp of this run.
        """
        self._comment_fetcher = comment_fetcher
        self._submissions_writer = submissions_writer
        self._comments_writer = comments_writer
        self._s3_prefix = s3_prefix
        self._run_timestamp = int(run_timestamp)

    def ingest_listing(
        self,
        subreddit_name: str,
        simple_fetcher: "SimpleListingFetcher",
        sort_type: str,
        time_filter: str = "all",
        max_items: int = 1000,
        include_comments: bool = True,
    ) -> RedditIngestionResult:
        """
        Ingest submissions from a single Reddit listing endpoint.

        Fetches up to max_items submissions from the specified listing (top or
        controversial) and writes them to S3. No deduplication is performed -
        downstream Glue ETL handles that.

        Args:
            subreddit_name: Name of the subreddit to ingest from.
            simple_fetcher: SimpleListingFetcher instance for fetching submissions.
            sort_type: Either "top" or "controversial".
            time_filter: Time filter for the listing ("all", "year", "month", etc.).
            max_items: Maximum number of submissions to fetch (max 1000).
            include_comments: Whether to fetch comments for each submission.

        Returns:
            RedditIngestionResult with counts and metadata.
        """
        LOGGER.info(
            "Starting listing ingestion: subreddit=%s sort_type=%s time_filter=%s max_items=%d",
            subreddit_name,
            sort_type,
            time_filter,
            max_items,
        )

        submissions_count = 0
        comments_count = 0

        for submission in simple_fetcher.fetch_listing(
            subreddit_name=subreddit_name,
            sort_type=sort_type,
            time_filter=time_filter,
            limit=max_items,
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
            sort_type=sort_type,
            time_filter=time_filter,
        )

        LOGGER.info(
            "Completed listing ingestion: subreddit=%s sort_type=%s submissions=%d comments=%d",
            subreddit_name,
            sort_type,
            submissions_count,
            comments_count,
        )

        return result

    def _write_submission(self, submission: Submission) -> None:
        """Serialize and write a single submission."""
        serialized = SubmissionSerializer.to_dict(
            submission,
            updated_at=self._run_timestamp,
        )
        self._submissions_writer.write(serialized)

    def _write_comments_for_submission(self, submission: Submission) -> int:
        """Fetch and write all comments for a submission."""
        written = 0
        for comment in self._comment_fetcher.iterate_comments(submission=submission):
            serialized = CommentSerializer.to_dict(
                comment,
                updated_at=self._run_timestamp,
            )
            self._comments_writer.write(serialized)
            written += 1
        return written
