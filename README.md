# 🎵 Hymnary Organ–Hymnal Crossover

Identifying which hymns on an Allen organ's console controller also appear in the church hymn book — so the organist knows exactly which built-in hymns are available for each service.

## Project Overview

An Allen organ has **182 built-in hymns** selectable from its console controller display. The church also uses a printed hymn book. This project:

1. **Scrapes tune metadata** from [hymnary.org](https://hymnary.org/) for each of the 182 organ hymns — collecting tune title, composer, meter, incipit, key, MIDI files, copyright, and associated texts (up to 5 tunes per hymn, ranked by popularity).
2. **Filters false positives with AI** — each scraped tune is classified as genuinely relevant or a search false-positive using OpenAI's GPT-5.2 with [structured outputs](https://platform.openai.com/docs/guides/structured-outputs). Classification is run 3 times per hymn with majority voting for reliability.
3. **Builds a structured index** mapping every organ hymn to its verified tunes, with full metadata.
4. **Exports results** as a formatted Excel workbook (with two sheets: tidy hymn–tune data and a per-hymn summary), filtered JSON, and CSV.
5. **Presents the results** on a [Quarto](https://quarto.org/) website deployable via GitHub Pages, including an Excel download for offline use.

## How the AI Screening Works

Searching Hymnary.org by hymn title often returns unrelated tunes (e.g. searching "A Mighty Fortress" also returns AUSTRIAN HYMN, HYFRYDOL, etc.). To separate genuine matches from noise, the pipeline uses an LLM-based classifier:

1. **Input** — For each hymn, the classifier receives the hymn title, all candidate tunes, and each tune's metadata (composer, meter, key, associated texts, instance percentages, editorial notes).
2. **Persona** — The LLM is prompted as a specialist hymnologist who understands the conventions linking hymn texts to tune names.
3. **Structured output** — Using OpenAI's Pydantic response format, the model returns a typed `HymnClassification` object with per-tune verdicts (`is_relevant`, `confidence`, `reasoning`).
4. **Majority voting** — Each hymn is classified 3 independent times. The final verdict for each tune is decided by majority vote (≥2/3 must agree).
5. **Checkpoint/resume** — Progress is saved after every hymn, so interrupted runs pick up where they left off.

## Attribution

> **All hymn and tune data is sourced from [Hymnary.org](https://hymnary.org/)**, a comprehensive index of hymns and hymnals maintained by the [Calvin Institute of Christian Worship](https://worship.calvin.edu/) and the [Christian Classics Ethereal Library (CCEL)](https://www.ccel.org/). This project uses their data for personal, non-commercial church ministry purposes. We are deeply grateful for this incredible resource.

## Project Structure

```
hymnary/
├── data/
│   ├── raw/                  # Cached HTML responses (gitignored)
│   ├── interim/              # Checkpoints for scraping & classification
│   └── processed/            # Final outputs
│       ├── hymn_tune_index.json          # Full scraped index (all tunes)
│       ├── hymn_tune_index_filtered.json # Filtered index (relevant only)
│       ├── classifications.json          # Full LLM reasoning log
│       ├── summary.csv                   # Flat CSV (all tunes)
│       ├── summary_filtered.csv          # Flat CSV with is_relevant column
│       └── hymn_tune_data.xlsx           # Formatted Excel workbook
├── src/                      # Python pipeline
│   ├── config.py             # URLs, paths, delay settings
│   ├── scraper.py            # HTTP requests with polite delays & caching
│   ├── parser.py             # BeautifulSoup HTML parsing (search + tune pages)
│   ├── pipeline.py           # Scraping orchestrator: CSV → search → parse → JSON
│   ├── classifier.py         # OpenAI LLM classifier (structured outputs + voting)
│   ├── filter_pipeline.py    # Classification orchestrator with checkpoint/resume
│   └── utils.py              # Logging, file I/O, retry helpers
├── site/                     # Quarto website source
│   ├── _quarto.yml           # Site configuration
│   ├── index.qmd             # Landing page with live stats
│   ├── hymns.qmd             # Searchable hymn–tune table with relevance badges
│   ├── about.qmd             # Attribution & methodology
│   ├── styles.css            # Custom styling (church-warm palette)
│   └── hymn_tune_data.xlsx   # Excel download (copied from data/processed/)
├── docs/                     # Rendered Quarto site (for GitHub Pages)
├── pyproject.toml            # uv project configuration
└── PLAN.md                   # Development progress tracker
```

## Setup

### Prerequisites

- [Python 3.12+](https://www.python.org/)
- [uv](https://docs.astral.sh/uv/) for Python package management
- [Quarto](https://quarto.org/docs/get-started/) for building the website
- An [OpenAI API key](https://platform.openai.com/api-keys) (for the AI classification step)

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd hymnary

# Install dependencies (creates .venv automatically)
uv sync

# Set your OpenAI API key
echo "OPENAI_API_KEY=sk-..." > .env
```

### Running the Pipeline

```bash
# Step 1: Scrape tune data from Hymnary.org (~45 min, cached after first run)
uv run python -m src.pipeline

# Step 2: Classify tunes with GPT-5.2 (~60 min, checkpoint/resume supported)
uv run python -m src.filter_pipeline

# Step 3: Build the Quarto website
cd site && quarto render
```

The classification step supports several options:

```bash
uv run python -m src.filter_pipeline --limit 5       # Test on first 5 hymns
uv run python -m src.filter_pipeline --reset          # Clear checkpoint, start fresh
uv run python -m src.filter_pipeline --model gpt-5.2  # Specify model (default: gpt-5.2)
uv run python -m src.filter_pipeline --num-runs 5     # More voting rounds (default: 3)
```

## Scraping Ethics

This scraper is designed to be **extremely respectful** of hymnary.org's resources:

- **15-second delay** between every HTTP request (3× their `robots.txt` crawl-delay of 5s)
- **Full HTML caching** — pages are fetched once and stored locally; re-runs use cached data
- **Resume support** — interrupted runs pick up where they left off
- **Polite User-Agent** identifying this as a personal church project
- **robots.txt compliant** — only accessing allowed paths (`/tune/`, `/text/`, `/search?qu=`)

## Output Formats

| File | Description |
|------|-------------|
| `hymn_tune_index.json` | Full scraped data — all tunes for all 182 hymns |
| `hymn_tune_index_filtered.json` | Only AI-verified relevant tunes |
| `classifications.json` | Complete LLM reasoning log (all runs, votes, confidence) |
| `summary_filtered.csv` | Tidy CSV — one row per hymn–tune pair, with `is_relevant` column |
| `hymn_tune_data.xlsx` | Formatted Excel — two sheets: full tidy data + per-hymn summary |

## License

This project is for personal, non-commercial church ministry use. Hymn data copyright belongs to the respective holders as noted in each tune's metadata. See [LICENSE](LICENSE) for code licensing.
