import os
from typing import Optional

import praw


class RedditClientFactory:
    """Factory to create a configured PRAW Reddit client from environment variables.

    Required environment variables:
    - REDDIT_CLIENT_ID
    - REDDIT_CLIENT_SECRET
    - REDDIT_USER_AGENT
    """

    ENV_CLIENT_ID = "REDDIT_CLIENT_ID"
    ENV_CLIENT_SECRET = "REDDIT_CLIENT_SECRET"
    ENV_USER_AGENT = "REDDIT_USER_AGENT"

    @classmethod
    def create(cls, request_timeout: Optional[int] = 30) -> praw.Reddit:
        client_id = os.environ.get(cls.ENV_CLIENT_ID)
        client_secret = os.environ.get(cls.ENV_CLIENT_SECRET)
        user_agent = os.environ.get(cls.ENV_USER_AGENT)

        missing = [
            name
            for name, val in [
                (cls.ENV_CLIENT_ID, client_id),
                (cls.ENV_CLIENT_SECRET, client_secret),
                (cls.ENV_USER_AGENT, user_agent),
            ]
            if not val
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise RuntimeError(
                f"Missing required environment variables: {missing_str}. "
                f"Set them before running. See README.md for details."
            )

        # Configure a read-only application client
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False,
            timeout=request_timeout,
        )

        # Lightweight sanity check; avoids network call
        if not hasattr(reddit, "subreddit"):
            raise RuntimeError("Failed to create PRAW Reddit client.")

        return reddit


