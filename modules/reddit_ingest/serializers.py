from __future__ import annotations

from typing import Any, Dict, Optional


def _safe_author_name(author_obj: Any) -> Optional[str]:
    try:
        return getattr(author_obj, "name", None)
    except Exception:
        return None


def _normalize_edited(edited: Any) -> Optional[int]:
    # PRAW uses False or a unix timestamp (float)
    if not edited:
        return None
    try:
        return int(edited)
    except Exception:
        return None


def _awards_summary(thing: Any) -> Dict[str, int]:
    try:
        awards = getattr(thing, "all_awardings", []) or []
    except Exception:
        awards = []
    summary: Dict[str, int] = {}
    for a in awards:
        name = a.get("name") if isinstance(a, dict) else getattr(a, "name", None)
        count = a.get("count") if isinstance(a, dict) else getattr(a, "count", 0)
        if name:
            summary[name] = int(count or 0)
    return summary


class SubmissionSerializer:
    """Serialize a PRAW Submission into a JSON-safe dict."""

    @staticmethod
    def to_dict(s: Any) -> Dict[str, Any]:
        subreddit_name = getattr(getattr(s, "subreddit", None), "display_name", None)
        return {
            "id": getattr(s, "id", None),
            "subreddit": subreddit_name,
            "author": _safe_author_name(getattr(s, "author", None)),
            "title": getattr(s, "title", "") or "",
            "selftext": getattr(s, "selftext", "") or "",
            "created_utc": int(getattr(s, "created_utc", 0) or 0),
            "score": int(getattr(s, "score", 0) or 0),
            "upvote_ratio": float(getattr(s, "upvote_ratio", 0.0) or 0.0),
            "num_comments": int(getattr(s, "num_comments", 0) or 0),
            "is_self": bool(getattr(s, "is_self", False)),
            "stickied": bool(getattr(s, "stickied", False)),
            "locked": bool(getattr(s, "locked", False)),
            "distinguished": getattr(s, "distinguished", None),
            "permalink": f"https://reddit.com{getattr(s, 'permalink', '')}",
            "url": getattr(s, "url", None),
            "edited": _normalize_edited(getattr(s, "edited", None)),
            "awards": _awards_summary(s),
            # Media hints (best-effort)
            "is_video": bool(getattr(s, "is_video", False)),
            "media_only": bool(getattr(s, "media_only", False)),
            "thumbnail": getattr(s, "thumbnail", None),
        }


class CommentSerializer:
    """Serialize a PRAW Comment into a JSON-safe dict."""

    @staticmethod
    def to_dict(c: Any) -> Dict[str, Any]:
        link_id = getattr(c, "link_id", None)
        parent_id = getattr(c, "parent_id", None)
        submission_id: Optional[str] = None
        if link_id and isinstance(link_id, str) and link_id.startswith("t3_"):
            submission_id = link_id[3:]
        else:
            submission = getattr(c, "submission", None)
            submission_id = getattr(submission, "id", None)
        return {
            "id": getattr(c, "id", None),
            "submission_id": submission_id,
            "link_id": link_id,
            "parent_id": parent_id,
            "author": _safe_author_name(getattr(c, "author", None)),
            "body": getattr(c, "body", "") or "",
            "created_utc": int(getattr(c, "created_utc", 0) or 0),
            "score": int(getattr(c, "score", 0) or 0),
            "controversiality": int(getattr(c, "controversiality", 0) or 0),
            "is_submitter": bool(getattr(c, "is_submitter", False)),
            "distinguished": getattr(c, "distinguished", None),
            "edited": _normalize_edited(getattr(c, "edited", None)),
            "permalink": f"https://reddit.com{getattr(c, 'permalink', '')}",
            "depth": int(getattr(c, "depth", 0) or 0),
            "awards": _awards_summary(c),
        }


