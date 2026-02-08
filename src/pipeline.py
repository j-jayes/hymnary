"""
Pipeline orchestrator: reads the input CSV, scrapes hymnary.org for each hymn,
parses the results, and writes structured JSON + CSV outputs.

Features:
  - Checkpoint/resume: tracks completed hymns so interrupted runs continue
  - All-tunes index: one JSON mapping each organ hymn to its tune(s)
  - Summary CSV: flat table for easy consumption by the Quarto site

Usage:
    uv run python -m src.pipeline            # process all hymns
    uv run python -m src.pipeline --limit 3  # process first 3 only (testing)
"""

from __future__ import annotations

import argparse
import csv
import sys
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import (
    CHECKPOINT_PATH,
    HYMN_TUNE_INDEX_PATH,
    INPUT_CSV,
    PROCESSED_DIR,
    SEARCH_INDEX_PATH,
    SUMMARY_CSV_PATH,
    TUNES_JSON_PATH,
)
from src.parser import parse_search_results, parse_tune_detail
from src.scraper import fetch_search_results, fetch_tune_page
from src.utils import (
    get_logger,
    load_checkpoint,
    make_safe_filename,
    read_json,
    save_checkpoint,
    title_to_search_query,
    write_json,
)

log = get_logger(__name__)


# ──────────────────────────────────────────────
# Input loading
# ──────────────────────────────────────────────

def load_input_hymns(path: Path | None = None) -> list[dict[str, str]]:
    """Load the organ hymn list from CSV.

    Returns a list of dicts with keys:
        - console_display: abbreviated name on the Allen organ
        - full_title: human-readable hymn title
        - hymn_key: filesystem-safe unique key
    """
    path = path or INPUT_CSV
    hymns = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            console = row["Console Controller Display"].strip()
            title = row["Full Hymn Title"].strip()
            hymns.append(
                {
                    "console_display": console,
                    "full_title": title,
                    "hymn_key": make_safe_filename(title),
                }
            )
    log.info("Loaded %d hymns from %s", len(hymns), path.name)
    return hymns


# ──────────────────────────────────────────────
# Per-hymn processing
# ──────────────────────────────────────────────

def process_hymn(hymn: dict[str, str]) -> dict[str, Any]:
    """Search, scrape, and parse all tunes for a single hymn.

    Returns a dict::

        {
            "console_display": "AMightyFortress",
            "full_title": "A Mighty Fortress",
            "hymn_key": "a_mighty_fortress",
            "search_query": "A+Mighty+Fortress",
            "tunes_found": [
                {
                    "tune_slug": "ein_feste_burg_luther",
                    "search_card": { ... },    # from search results
                    "detail": { ... },          # from tune detail page
                },
                ...
            ],
        }
    """
    title = hymn["full_title"]
    key = hymn["hymn_key"]
    query = title_to_search_query(title)

    log.info("━" * 50)
    log.info("Processing: %s  (key=%s)", title, key)
    log.info("  Search query: %s", query)

    # Step 1: Search for tunes
    search_html = fetch_search_results(query, key)
    tune_cards = parse_search_results(search_html)

    total_search_results = len(tune_cards)
    log.info("  Found %d tune(s) in search results", total_search_results)

    # Limit to the top MAX_TUNES_PER_HYMN tunes (sorted by hymnal count)
    # to avoid excessive requests for hymns with many loose matches.
    MAX_TUNES_PER_HYMN = 5
    tune_cards.sort(key=lambda c: c.get("num_hymnals", 0), reverse=True)
    if len(tune_cards) > MAX_TUNES_PER_HYMN:
        log.info(
            "  Limiting to top %d tunes (by hymnal count)", MAX_TUNES_PER_HYMN
        )
        tune_cards = tune_cards[:MAX_TUNES_PER_HYMN]

    # Step 2: Fetch detail page for each tune
    tunes_data: list[dict[str, Any]] = []
    for card in tune_cards:
        slug = card["tune_slug"]
        if not slug:
            log.warning("  Skipping card with no slug: %s", card.get("title"))
            continue

        log.info("  Fetching tune detail: %s", slug)
        try:
            detail_html = fetch_tune_page(slug)
            detail = parse_tune_detail(detail_html)
        except Exception as exc:
            log.error("  ✗ Failed to fetch/parse tune %s: %s", slug, exc)
            detail = {"error": str(exc)}

        tunes_data.append(
            {
                "tune_slug": slug,
                "search_card": card,
                "detail": detail,
            }
        )

    return {
        "console_display": hymn["console_display"],
        "full_title": title,
        "hymn_key": key,
        "search_query": query,
        "total_search_results": total_search_results,
        "tunes_found": tunes_data,
    }


# ──────────────────────────────────────────────
# Output writing
# ──────────────────────────────────────────────

def build_outputs(all_results: list[dict[str, Any]]) -> None:
    """Write the master index, tunes dict, and summary CSV."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # ── Master index: hymn → tunes ──
    write_json(HYMN_TUNE_INDEX_PATH, all_results)
    log.info("Wrote master index: %s", HYMN_TUNE_INDEX_PATH)

    # ── Tunes dict: slug → detail (deduplicated) ──
    tunes_dict: dict[str, Any] = {}
    for hymn in all_results:
        for t in hymn["tunes_found"]:
            slug = t["tune_slug"]
            if slug and slug not in tunes_dict:
                tunes_dict[slug] = t["detail"]
    write_json(TUNES_JSON_PATH, tunes_dict)
    log.info("Wrote %d unique tunes: %s", len(tunes_dict), TUNES_JSON_PATH)

    # ── Search index: hymn_key → [tune slugs] ──
    search_index = {
        h["hymn_key"]: [t["tune_slug"] for t in h["tunes_found"]]
        for h in all_results
    }
    write_json(SEARCH_INDEX_PATH, search_index)
    log.info("Wrote search index: %s", SEARCH_INDEX_PATH)

    # ── Flat summary CSV for the Quarto site ──
    rows = []
    for hymn in all_results:
        for t in hymn["tunes_found"]:
            detail = t.get("detail", {})
            rows.append(
                {
                    "console_display": hymn["console_display"],
                    "full_title": hymn["full_title"],
                    "total_search_results": hymn.get("total_search_results", 0),
                    "tune_title": detail.get("title", t.get("search_card", {}).get("title", "")),
                    "tune_slug": t["tune_slug"],
                    "composer": detail.get("composer", ""),
                    "meter": detail.get("meter", ""),
                    "incipit": detail.get("incipit", ""),
                    "key": detail.get("key", ""),
                    "place_of_origin": detail.get("place_of_origin", ""),
                    "copyright": detail.get("copyright", ""),
                    "num_hymnals": detail.get("num_hymnals", 0),
                    "midi_url": detail.get("midi_url", ""),
                    "recording_url": detail.get("recording_url", ""),
                    "pdf_url": detail.get("pdf_url", ""),
                    "hymnary_url": detail.get(
                        "hymnary_url",
                        f"https://hymnary.org/tune/{t['tune_slug']}",
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(SUMMARY_CSV_PATH, index=False)
        log.info("Wrote summary CSV (%d rows): %s", len(rows), SUMMARY_CSV_PATH)
    else:
        log.warning("No tune data to write to summary CSV.")


# ──────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────

def run(limit: int | None = None) -> None:
    """Run the full scraping pipeline with checkpoint/resume."""
    hymns = load_input_hymns()
    if limit:
        hymns = hymns[:limit]
        log.info("Limiting to first %d hymns", limit)

    # Load existing results + checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_PATH)
    completed_keys = set(checkpoint["completed"])

    existing_results: list[dict[str, Any]] = read_json(HYMN_TUNE_INDEX_PATH) or []
    existing_by_key = {r["hymn_key"]: r for r in existing_results}

    total = len(hymns)
    skipped = 0
    processed = 0

    for i, hymn in enumerate(hymns, 1):
        key = hymn["hymn_key"]

        if key in completed_keys:
            log.info("[%d/%d] Skipping (already done): %s", i, total, hymn["full_title"])
            skipped += 1
            continue

        try:
            result = process_hymn(hymn)
            existing_by_key[key] = result
            # Only mark as completed AFTER successful processing
            if key not in completed_keys:
                checkpoint["completed"].append(key)
                completed_keys.add(key)
            processed += 1

        except KeyboardInterrupt:
            log.warning("\n⚠ Interrupted by user. Progress saved — resume by re-running.")
            save_checkpoint(CHECKPOINT_PATH, checkpoint)
            all_results = list(existing_by_key.values())
            if all_results:
                build_outputs(all_results)
            sys.exit(0)

        except Exception as exc:
            log.error("[%d/%d] ✗ FAILED: %s — %s", i, total, hymn["full_title"], exc)
            log.error("  %s", traceback.format_exc().splitlines()[-1])
            checkpoint["failed"][key] = str(exc)

        # Save checkpoint after every hymn (so we can resume)
        save_checkpoint(CHECKPOINT_PATH, checkpoint)

        # Periodically write outputs (every 5 hymns or at the end)
        if processed % 5 == 0 or i == total:
            all_results = list(existing_by_key.values())
            build_outputs(all_results)

    # Final output write
    all_results = list(existing_by_key.values())
    build_outputs(all_results)

    log.info("━" * 50)
    log.info("Pipeline complete!")
    log.info("  Total hymns: %d", total)
    log.info("  Processed:   %d", processed)
    log.info("  Skipped:     %d (already cached)", skipped)
    log.info("  Failed:      %d", len(checkpoint["failed"]))
    if checkpoint["failed"]:
        for k, err in checkpoint["failed"].items():
            log.info("    ✗ %s: %s", k, err)


# ──────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape hymnary.org tune data for Allen organ hymns."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N hymns (for testing).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear checkpoint and reprocess everything (cached HTML is kept).",
    )
    args = parser.parse_args()

    if args.reset:
        CHECKPOINT_PATH.unlink(missing_ok=True)
        HYMN_TUNE_INDEX_PATH.unlink(missing_ok=True)
        log.info("Checkpoint cleared. Will reprocess all hymns.")

    run(limit=args.limit)


if __name__ == "__main__":
    main()
