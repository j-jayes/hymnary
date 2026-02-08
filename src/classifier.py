"""
LLM-based relevance classifier for hymn–tune matches.

Uses OpenAI's structured outputs (Pydantic response models) to classify each
scraped tune as relevant or a false-positive for its hymn.

The classification is run multiple times (default 3) per hymn and the final
verdict is decided by majority vote for reliability.
"""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

from src.utils import get_logger

# ──────────────────────────────────────────────
# Load API key from .env in project root
# ──────────────────────────────────────────────

load_dotenv()

log = get_logger("classifier")

# ──────────────────────────────────────────────
# Pydantic response models (structured output)
# ──────────────────────────────────────────────


class TuneRelevance(BaseModel):
    """Classification of a single tune's relevance to a hymn."""

    tune_slug: str = Field(
        description="The unique slug identifying this tune (must match the input data)."
    )
    is_relevant: bool = Field(
        description=(
            "True if this tune is genuinely associated with the hymn — "
            "i.e. it is a well-known musical setting of this hymn text. "
            "False if it is a false-positive from the search."
        )
    )
    confidence: str = Field(
        description="How confident the classification is: 'high', 'medium', or 'low'."
    )
    reasoning: str = Field(
        description=(
            "A concise (1–3 sentence) explanation of why this tune is or is not "
            "relevant to the hymn. Reference the evidence you used."
        )
    )


class HymnClassification(BaseModel):
    """Full classification result for one hymn and all its candidate tunes."""

    hymn_key: str = Field(description="The unique key for this hymn.")
    classifications: list[TuneRelevance] = Field(
        description="One classification entry per tune candidate."
    )


# ──────────────────────────────────────────────
# System prompt
# ──────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert hymnologist and church music scholar. Your task is to classify
whether each candidate tune is genuinely associated with a given hymn.

CONTEXT:
We have an Allen organ with 182 built-in hymns. For each hymn we searched
Hymnary.org by title to find associated tunes. Because the search is broad,
many results are FALSE POSITIVES — tunes that happen to match some words in
the hymn title but are not actually musical settings of that hymn text.

YOUR JOB:
For each tune candidate, decide if it is a genuine musical setting of the hymn
(is_relevant = true) or a false positive (is_relevant = false).

KEY EVIDENCE TO USE:
1. **associated_texts** — If the hymn title (or a close variant) appears in the
   tune's associated texts, the tune is almost certainly relevant.
2. **instance_percentages** — If the hymn title appears with a significant
   percentage, this confirms the tune is commonly used with that hymn.
3. **used_with_text** (from the search card) — States which text the tune is
   primarily associated with. If it names the hymn, that's strong evidence.
4. **Tune name vs hymn title** — Some tunes share a name or obvious link with
   the hymn (e.g. "EIN FESTE BURG" for "A Mighty Fortress").
5. **Meter compatibility** — If the meter of a tune could plausibly fit the
   hymn's typical meter, that is supporting (but not conclusive) evidence.
6. **Your own knowledge of hymn repertoire** — You are an expert and may know
   which tunes are standard settings for well-known hymns.

GUIDELINES:
- Be fairly GENEROUS: if a tune is a recognised (even if uncommon) setting of
  the hymn, mark it relevant.
- But filter out obvious noise: a tune whose associated texts and percentages
  show it is primarily used with a COMPLETELY DIFFERENT hymn is a false positive.
- When in doubt, lean towards relevant if there is any credible evidence.
"""


# ──────────────────────────────────────────────
# Build the user message for one hymn
# ──────────────────────────────────────────────


def _build_user_message(hymn: dict[str, Any]) -> str:
    """Assemble a rich user message with all tune evidence for one hymn."""
    title = hymn["full_title"]
    hymn_key = hymn["hymn_key"]
    total = hymn.get("total_search_results", "?")

    lines = [
        f'# Hymn: "{title}" (key: {hymn_key})',
        f"Total search results on Hymnary.org: {total}",
        f"We kept the top {len(hymn['tunes_found'])} tunes by popularity.\n",
        "---",
        "",
    ]

    for i, tune in enumerate(hymn["tunes_found"], 1):
        card = tune.get("search_card", {})
        detail = tune.get("detail", {})
        slug = tune["tune_slug"]

        lines.append(f"## Tune {i}: {detail.get('title', card.get('title', slug))}")
        lines.append(f"- **tune_slug**: `{slug}`")
        lines.append(f"- **composer**: {detail.get('composer', card.get('composer', '—'))}")
        lines.append(f"- **meter**: {detail.get('meter', card.get('meter', '—'))}")
        lines.append(f"- **key**: {detail.get('key', card.get('tune_key', '—'))}")
        lines.append(f"- **num_hymnals**: {detail.get('num_hymnals', card.get('num_hymnals', '—'))}")

        uwt = card.get("used_with_text", "")
        if uwt:
            lines.append(f"- **used_with_text** (search card): {uwt}")

        at = detail.get("associated_texts", [])
        if at:
            names = [t["name"] for t in at]
            lines.append(f"- **associated_texts**: {'; '.join(names)}")

        ip = detail.get("instance_percentages", [])
        if ip:
            pct_strs = [f"{name} ({pct:.1f}%)" for name, pct, *_ in ip]
            lines.append(f"- **instance_percentages**: {'; '.join(pct_strs)}")

        notes = detail.get("notes", "")
        if notes:
            # Truncate long notes to keep token count reasonable
            if len(notes) > 400:
                notes = notes[:400] + "…"
            lines.append(f"- **notes**: {notes}")

        lines.append("")

    lines.append("---")
    lines.append(
        f"For EACH tune above, classify it as relevant or not relevant to the "
        f'hymn "{title}". Return the hymn_key as "{hymn_key}".'
    )

    return "\n".join(lines)


# ──────────────────────────────────────────────
# Single classification call
# ──────────────────────────────────────────────


def classify_hymn_once(
    client: OpenAI,
    hymn: dict[str, Any],
    *,
    model: str = "gpt-5.2",
    temperature: float = 0.3,
) -> HymnClassification:
    """Run a single classification call for one hymn.

    Returns a validated HymnClassification via OpenAI structured outputs.
    """
    user_msg = _build_user_message(hymn)

    completion = client.chat.completions.parse(
        model=model,
        messages=[
            {"role": "developer", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format=HymnClassification,
        temperature=temperature,
    )

    result = completion.choices[0].message

    if result.refusal:
        raise ValueError(f"Model refused to classify: {result.refusal}")

    if result.parsed is None:
        raise ValueError("Model returned no parsed output.")

    return result.parsed


# ──────────────────────────────────────────────
# Majority-vote classification (N runs)
# ──────────────────────────────────────────────


def classify_hymn(
    client: OpenAI,
    hymn: dict[str, Any],
    *,
    model: str = "gpt-5.2",
    num_runs: int = 3,
    temperature: float = 0.3,
) -> dict[str, Any]:
    """Classify a hymn's tunes with majority voting over *num_runs* calls.

    Returns a dict containing:
        - hymn_key
        - full_title
        - runs: list of raw HymnClassification dicts (one per run)
        - consensus: list of final TuneRelevance dicts with majority vote
    """
    hymn_key = hymn["hymn_key"]
    full_title = hymn["full_title"]
    tune_slugs = [t["tune_slug"] for t in hymn["tunes_found"]]

    # Collect votes from each run
    all_runs: list[dict[str, Any]] = []
    # slug → list of bool votes
    votes: dict[str, list[bool]] = {s: [] for s in tune_slugs}
    # slug → list of (confidence, reasoning) per run
    reasoning_log: dict[str, list[dict[str, str]]] = {s: [] for s in tune_slugs}

    for run_idx in range(num_runs):
        log.info("    Run %d/%d for %s", run_idx + 1, num_runs, full_title)
        result = classify_hymn_once(client, hymn, model=model, temperature=temperature)
        run_dict = result.model_dump()
        all_runs.append(run_dict)

        for clf in result.classifications:
            slug = clf.tune_slug
            if slug in votes:
                votes[slug].append(clf.is_relevant)
                reasoning_log[slug].append(
                    {"confidence": clf.confidence, "reasoning": clf.reasoning}
                )

    # Majority vote: relevant if > half the runs said True
    threshold = num_runs / 2
    consensus: list[dict[str, Any]] = []
    for slug in tune_slugs:
        v = votes[slug]
        relevant_count = sum(v)
        is_relevant = relevant_count > threshold

        # Pick the reasoning from the majority side
        majority_reasons = [
            r
            for r, vote in zip(reasoning_log[slug], v)
            if vote == is_relevant
        ]
        best_reason = majority_reasons[0] if majority_reasons else {"confidence": "low", "reasoning": "No consensus."}

        consensus.append(
            {
                "tune_slug": slug,
                "is_relevant": is_relevant,
                "vote_count": relevant_count,
                "total_runs": num_runs,
                "confidence": best_reason["confidence"],
                "reasoning": best_reason["reasoning"],
            }
        )

    return {
        "hymn_key": hymn_key,
        "full_title": full_title,
        "runs": all_runs,
        "consensus": consensus,
    }


def get_client() -> OpenAI:
    """Create an OpenAI client, raising a clear error if the key is missing."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key.startswith("your_"):
        raise RuntimeError(
            "OPENAI_API_KEY not set. Add it to .env in the project root."
        )
    return OpenAI(api_key=api_key)
