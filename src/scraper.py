"""
Polite HTTP scraper for hymnary.org.

Every request:
  - Waits REQUEST_DELAY_SECONDS (15 s) before firing
  - Uses a descriptive User-Agent header
  - Caches the raw HTML to disk so re-runs never re-fetch
  - Retries with exponential backoff on transient errors

Cached files are stored under data/raw/search_results/ and data/raw/tune_pages/.
"""

from __future__ import annotations

import time
from pathlib import Path

import requests

from src.config import (
    HEADERS,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_FACTOR,
    SEARCH_CACHE_DIR,
    SEARCH_URL,
    TEXT_SEARCH_URL,
    TUNE_CACHE_DIR,
    TUNE_URL,
)
from src.utils import get_logger, make_safe_filename

log = get_logger(__name__)

# Module-level session for connection reuse
_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """Lazily create a requests session with our headers."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


# ──────────────────────────────────────────────
# Core fetch with cache, delay, and retry
# ──────────────────────────────────────────────

def _fetch(url: str, cache_path: Path) -> str:
    """Fetch a URL, returning cached content if available.

    On a cache miss:
      1. Sleeps REQUEST_DELAY_SECONDS
      2. GETs the URL with retries
      3. Saves the response body to cache_path
    """
    # ── Cache hit ──
    if cache_path.exists():
        log.info("  Cache hit: %s", cache_path.name)
        return cache_path.read_text(encoding="utf-8")

    # ── Polite delay ──
    log.info("  Waiting %d s before request …", REQUEST_DELAY_SECONDS)
    time.sleep(REQUEST_DELAY_SECONDS)

    # ── Fetch with retries ──
    session = _get_session()
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("  GET %s (attempt %d/%d)", url, attempt, MAX_RETRIES)
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # ── Cache the response ──
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(resp.text, encoding="utf-8")
            log.info("  ✓ Cached to %s (%d bytes)", cache_path.name, len(resp.text))
            return resp.text

        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_FACTOR ** attempt
                log.warning(
                    "  ⚠ Request failed (%s), retrying in %d s …", exc, backoff
                )
                time.sleep(backoff)

    raise RuntimeError(
        f"Failed to fetch {url} after {MAX_RETRIES} attempts: {last_exc}"
    )


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────

def fetch_search_results(query: str, hymn_key: str) -> str:
    """Search hymnary.org for tunes matching a query string.

    Args:
        query: URL-encoded search query (spaces as '+').
        hymn_key: A safe filename key for caching.

    Returns:
        Raw HTML of the search results page.
    """
    url = SEARCH_URL.format(query=query)
    cache = SEARCH_CACHE_DIR / f"{hymn_key}_search.html"
    return _fetch(url, cache)


def fetch_text_search_results(query: str, hymn_key: str) -> str:
    """Fallback: search for texts instead of tunes.

    Useful when a tune search returns no results but the hymn title
    matches a text entry that links to tunes.
    """
    url = TEXT_SEARCH_URL.format(query=query)
    cache = SEARCH_CACHE_DIR / f"{hymn_key}_textsearch.html"
    return _fetch(url, cache)


def fetch_tune_page(tune_slug: str) -> str:
    """Fetch the full tune detail page for a given slug.

    Args:
        tune_slug: e.g. "ein_feste_burg_luther"

    Returns:
        Raw HTML of the tune detail page.
    """
    url = TUNE_URL.format(slug=tune_slug)
    cache = TUNE_CACHE_DIR / f"{tune_slug}.html"
    return _fetch(url, cache)
