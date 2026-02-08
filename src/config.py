"""
Configuration constants for the hymnary scraping pipeline.

All paths, URLs, delay settings, and HTTP headers are centralised here
so the rest of the codebase stays clean and declarative.
"""

from pathlib import Path

# ──────────────────────────────────────────────
# Project paths
# ──────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# Cached HTML goes here (gitignored)
SEARCH_CACHE_DIR = RAW_DIR / "search_results"
TUNE_CACHE_DIR = RAW_DIR / "tune_pages"

# Input data
INPUT_CSV = PROJECT_ROOT / "data" / "raw" / "input_hymns.csv"

# Output artefacts
SEARCH_INDEX_PATH = INTERIM_DIR / "search_index.json"
TUNES_JSON_PATH = PROCESSED_DIR / "tunes.json"
HYMN_TUNE_INDEX_PATH = PROCESSED_DIR / "hymn_tune_index.json"
SUMMARY_CSV_PATH = PROCESSED_DIR / "summary.csv"

# Pipeline checkpoint (tracks which hymns have been fully processed)
CHECKPOINT_PATH = INTERIM_DIR / "checkpoint.json"

# ──────────────────────────────────────────────
# Hymnary.org URLs
# ──────────────────────────────────────────────

BASE_URL = "https://hymnary.org"

# Search for tunes by hymn title
# Usage: SEARCH_URL.format(query="a+mighty+fortress")
SEARCH_URL = BASE_URL + "/search?qu={query}+in%3Atunes"

# Tune detail page
# Usage: TUNE_URL.format(slug="ein_feste_burg_luther")
TUNE_URL = BASE_URL + "/tune/{slug}"

# Text detail page (fallback if search yields no tune results)
TEXT_SEARCH_URL = BASE_URL + "/search?qu={query}+in%3Atexts"

# ──────────────────────────────────────────────
# HTTP settings
# ──────────────────────────────────────────────

# Polite delay between requests — 3× the robots.txt Crawl-delay of 5s
REQUEST_DELAY_SECONDS = 15

# Identify ourselves honestly
USER_AGENT = (
    "HymnaryChurchProject/1.0 "
    "(personal non-commercial church ministry use; "
    "contact: j0nathanjayes@gmail.com)"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 30

# Maximum retries for failed requests
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2  # exponential backoff: 2, 4, 8 seconds
