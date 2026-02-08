"""
HTML parsing functions for hymnary.org pages.

Two page types are parsed:
  1. Search results — extracts tune cards (title, slug, meter, etc.)
  2. Tune detail   — extracts full metadata from the info table + extras

Every function takes a BeautifulSoup object and returns plain dicts
so the caller never has to think about HTML.
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup, Tag


# ──────────────────────────────────────────────
# Search results page
# ──────────────────────────────────────────────

def parse_search_results(html: str) -> list[dict[str, Any]]:
    """Parse a hymnary.org search results page for tune cards.

    Returns a list of dicts, each representing a tune result::

        {
            "title": "EIN FESTE BURG (ISORHYTHMIC)",
            "tune_slug": "ein_feste_burg_luther",
            "tune_url": "https://hymnary.org/tune/ein_feste_burg_luther",
            "meter": "8.7.8.7.6.6.6.6.7",
            "num_hymnals": 742,
            "composer": "Martin Luther; Johann S. Bach, 1685-1750",
            "tune_key": "C Major",
            "incipit": "11156 71765 17656",
            "used_with_text": "A Mighty Fortress Is Our God",
        }
    """
    soup = BeautifulSoup(html, "lxml")
    tunes: list[dict[str, Any]] = []

    # Two page layouts are possible:
    #
    # 1. Combined search (no `in:tunes` filter) — results are grouped under
    #    headings like "Texts", "Tunes", "Instances", "People".  We only want
    #    cards in the "Tunes" group.
    #
    # 2. Tune-filtered search (`in:tunes`) — ALL result cards are tunes and
    #    there are NO group headers at all.
    #
    # Detection: if there are any group-head cards, use group-based parsing;
    # otherwise treat every normal card as a tune.

    group_heads = soup.select("div.resultcard-grouphead")
    has_groups = len(group_heads) > 0

    if has_groups:
        # ── Grouped layout ──
        current_group = ""
        for card in soup.select("div.resultcard"):
            if "resultcard-grouphead" in card.get("class", []):
                h2 = card.select_one("h2")
                current_group = h2.text.strip() if h2 else ""
                continue
            if "resultcard-tiny" in card.get("class", []):
                continue
            if current_group != "Tunes":
                continue
            tune = _parse_tune_card(card)
            if tune:
                tunes.append(tune)
    else:
        # ── Flat layout (tune-filtered search) ──
        for card in soup.select("div.resultcard.resultcard-normal"):
            tune = _parse_tune_card(card)
            if tune:
                tunes.append(tune)

    return tunes


def _parse_tune_card(card: Tag) -> dict[str, Any] | None:
    """Extract tune info from a single search-result card."""
    title_link = card.select_one("h2 > a")
    if not title_link:
        return None

    title = title_link.text.strip()
    href = title_link.get("href", "")

    # Extract canonical tune slug from the href
    # Pattern: /result/.../tune/{slug}  or  /tune/{slug}
    slug_match = re.search(r"/tune/([a-z0-9_]+)", href)
    tune_slug = slug_match.group(1) if slug_match else ""

    tune_url = f"https://hymnary.org/tune/{tune_slug}" if tune_slug else href

    # Extract data-fieldname spans
    fields: dict[str, str] = {}
    for span in card.select("span[data-fieldname]"):
        fname = span.get("data-fieldname", "")
        # Strip the field label prefix (e.g. "Meter: ")
        text = span.text.strip()
        label = span.select_one("b.fieldLabel")
        if label:
            text = text.replace(label.text, "", 1).strip()
        fields[fname] = text

    # Parse num_hymnals from "Appears in N hymnals"
    num_hymnals = 0
    total_str = fields.get("total", "")
    m = re.search(r"(\d+)", total_str)
    if m:
        num_hymnals = int(m.group(1))

    return {
        "title": title,
        "tune_slug": tune_slug,
        "tune_url": tune_url,
        "meter": fields.get("meter", "").replace("Meter:", "").strip(),
        "num_hymnals": num_hymnals,
        "composer": fields.get("Composer and/or Arranger", "")
        .replace("Composer and/or  Arranger:", "")
        .replace("Composer and/or Arranger:", "")
        .strip(),
        "tune_key": fields.get("tuneKey", "").replace("Tune Key:", "").strip(),
        "incipit": fields.get("incipit", "").replace("Incipit:", "").strip(),
        "used_with_text": fields.get("usedWithText", "")
        .replace("Used With Text:", "")
        .strip(),
    }


# ──────────────────────────────────────────────
# Tune detail page
# ──────────────────────────────────────────────

def parse_tune_detail(html: str) -> dict[str, Any]:
    """Parse a hymnary.org /tune/{slug} page for full metadata.

    Returns a dict with all available tune information::

        {
            "title": "EIN FESTE BURG",
            "composer": "Martin Luther (1529)",
            "place_of_origin": "Germany",
            "meter": "8.7.8.7.6.6.6.6.7",
            "incipit": "11156 71765 17656",
            "key": "C Major/D Major",
            "copyright": "Public Domain",
            "num_hymnals": 743,
            "associated_texts": [...],
            "alternative_tunes": [...],
            "notes": "...",
            "hymnary_url": "https://hymnary.org/tune/ein_feste_burg_luther",
        }
    """
    soup = BeautifulSoup(html, "lxml")
    result: dict[str, Any] = {}

    # ── Page title ──
    h1 = soup.select_one("h1")
    result["title"] = h1.text.strip() if h1 else ""

    # ── Tune slug from body class ──
    body = soup.select_one("body")
    if body:
        classes = body.get("class", [])
        for cls in classes:
            if isinstance(cls, str) and cls.startswith("page-tune-"):
                slug = cls.replace("page-tune-", "").replace("-", "_")
                result["hymnary_url"] = f"https://hymnary.org/tune/{slug}"
                result["tune_slug"] = slug
                break

    # ── Info table (#at_tuneinfo) ──
    info_section = soup.select_one("#at_tuneinfo")
    if info_section:
        for row in info_section.select("tr.result-row"):
            label_el = row.select_one("span.hy_infoLabel")
            item_el = row.select_one("span.hy_infoItem")
            if not label_el or not item_el:
                continue
            label = label_el.text.strip().rstrip(":")
            value = item_el.text.strip()

            # Map labels to our normalised keys
            key_map = {
                "Title": "title",
                "Composer": "composer",
                "Place Of Origin": "place_of_origin",
                "Meter": "meter",
                "Incipit": "incipit",
                "Key": "key",
                "Copyright": "copyright",
                "Date": "date",
                "Source": "source",
                "Alternate Title": "alternate_title",
            }
            mapped = key_map.get(label)
            if mapped:
                result[mapped] = value
            else:
                # Preserve unknown fields in a catch-all
                result.setdefault("extra_fields", {})[label] = value

    # ── Above fold — hymnal count + media links ──
    above = soup.select_one("#authority_above_fold")
    if above:
        text = above.get_text(" ", strip=True)
        m = re.search(r"Published in (\d[\d,]*) hymnals?", text)
        if m:
            result["num_hymnals"] = int(m.group(1).replace(",", ""))

        # Extract primary media links (MIDI, PDF, Recording, MusicXML)
        # These appear as labelled links in the above-fold area.
        media_links: dict[str, str] = {}
        for a in above.select("a[href*='media/fetch']"):
            link_text = a.text.strip().lower()
            href = a.get("href", "")
            if not href:
                continue
            # Normalise to absolute URL
            if not href.startswith("http"):
                href = f"https://hymnary.org{href}"
            # Map by type — only keep the first (primary) of each
            if "midi" in link_text and "midi_url" not in media_links:
                media_links["midi_url"] = href
            elif "pdf" in link_text and "pdf_url" not in media_links:
                media_links["pdf_url"] = href
            elif "recording" in link_text and "recording_url" not in media_links:
                media_links["recording_url"] = href
            elif "musicxml" in link_text and "musicxml_url" not in media_links:
                media_links["musicxml_url"] = href
        result.update(media_links)

    # ── Associated texts (#at_texts) ──
    texts_section = soup.select_one("#at_texts")
    if texts_section:
        associated_texts = []
        for link in texts_section.select("a"):
            href = link.get("href", "")
            if "/text/" in href:
                text_name = link.text.strip()
                if text_name and text_name != "Go to text page...":
                    # Extract text slug
                    slug_match = re.search(r"/text/([a-z0-9_]+)", href)
                    associated_texts.append(
                        {
                            "name": text_name,
                            "slug": slug_match.group(1) if slug_match else "",
                            "url": href if href.startswith("http") else f"https://hymnary.org{href}",
                        }
                    )
        result["associated_texts"] = associated_texts

    # ── Alternative tunes (#at_alternatives) ──
    alts_section = soup.select_one("#at_alternatives")
    if alts_section:
        alternative_tunes = []
        for link in alts_section.select("a"):
            href = link.get("href", "")
            if "/tune/" in href:
                slug_match = re.search(r"/tune/([a-z0-9_]+)", href)
                alternative_tunes.append(
                    {
                        "name": link.text.strip(),
                        "slug": slug_match.group(1) if slug_match else "",
                        "url": href if href.startswith("http") else f"https://hymnary.org{href}",
                    }
                )
        result["alternative_tunes"] = alternative_tunes

    # ── Notes (#notes_content) ──
    notes_el = soup.select_one("#notes_content")
    if notes_el:
        result["notes"] = notes_el.get_text(" ", strip=True)

    # ── Instance percentage data (embedded JS) ──
    # var instancePercentages = [["A Mighty Fortress",56.27,"slug"],...]
    scripts = soup.find_all("script")
    for script in scripts:
        if script.string and "instancePercentages" in script.string:
            m = re.search(
                r"var instancePercentages\s*=\s*(\[.+?\]);",
                script.string,
                re.DOTALL,
            )
            if m:
                try:
                    import json

                    result["instance_percentages"] = json.loads(m.group(1))
                except (json.JSONDecodeError, ValueError):
                    pass
            break

    return result


# ──────────────────────────────────────────────
# Convenience: extract tune slugs from search
# ──────────────────────────────────────────────

def extract_tune_slugs_from_search(html: str) -> list[str]:
    """Return just the tune slugs from a search results page."""
    tunes = parse_search_results(html)
    return [t["tune_slug"] for t in tunes if t["tune_slug"]]
