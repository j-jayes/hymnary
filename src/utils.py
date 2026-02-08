"""
Utility helpers for the hymnary scraping pipeline.

Provides:
- Structured logging setup
- JSON read/write with atomic writes
- Checkpoint management for resume support
- Text normalisation for search queries
"""

import json
import logging
import re
import sys
import tempfile
from pathlib import Path
from typing import Any


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

def get_logger(name: str = "hymnary") -> logging.Logger:
    """Return a consistently-configured logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s │ %(levelname)-8s │ %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


# ──────────────────────────────────────────────
# JSON I/O (atomic writes to avoid corruption)
# ──────────────────────────────────────────────

def read_json(path: Path) -> Any:
    """Read a JSON file, returning None if it doesn't exist."""
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    """Write data to a JSON file atomically (write to temp, then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=path.parent, suffix=".tmp", prefix=path.stem
    )
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        Path(tmp_path).replace(path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


# ──────────────────────────────────────────────
# Checkpoint management
# ──────────────────────────────────────────────

def load_checkpoint(path: Path) -> dict:
    """Load the pipeline checkpoint.

    Returns a dict of the form:
        {
            "completed": ["hymn_key_1", "hymn_key_2", ...],
            "failed": {"hymn_key_3": "error message", ...}
        }
    """
    data = read_json(path)
    if data is None:
        return {"completed": [], "failed": {}}
    return data


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    """Persist the pipeline checkpoint."""
    write_json(path, checkpoint)


# ──────────────────────────────────────────────
# Text normalisation
# ──────────────────────────────────────────────

def normalise_hymn_title(title: str) -> str:
    """Clean a hymn title for use as a search query.

    Strips A/B variant suffixes, removes extraneous punctuation,
    and normalises whitespace.

    Examples:
        "Away in a Manger - A"  → "Away in a Manger"
        "Hark! The Herald Angels Sing" → "Hark The Herald Angels Sing"
        "Jesus, Lover of My Soul - B" → "Jesus Lover of My Soul"
    """
    # Strip variant suffixes like " - A", " - B", " -A", " -B"
    title = re.sub(r"\s*-\s*[AB]$", "", title.strip())
    # Remove punctuation that would confuse search (keep apostrophes in words)
    title = re.sub(r"[!?,;:\"'()]", "", title)
    # Collapse multiple spaces
    title = re.sub(r"\s+", " ", title).strip()
    return title


def title_to_search_query(title: str) -> str:
    """Convert a hymn title to a URL-safe search query string.

    Spaces become '+' for hymnary.org's search syntax.
    """
    cleaned = normalise_hymn_title(title)
    return cleaned.replace(" ", "+")


def make_safe_filename(text: str) -> str:
    """Convert arbitrary text to a filesystem-safe filename."""
    safe = re.sub(r"[^\w\s-]", "", text)
    safe = re.sub(r"\s+", "_", safe).strip("_")
    return safe.lower()
