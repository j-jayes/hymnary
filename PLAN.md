# Development Plan

## Progress Tracker

### Phase 1: Project Setup
- [x] Check robots.txt compliance (15s delay is 3× their crawl-delay: 5)
- [x] Initialize uv project with .venv
- [x] Add dependencies (requests, beautifulsoup4, lxml, pandas)
- [x] Create project directory structure
- [x] Write README.md with attribution
- [x] Write .gitignore

### Phase 2: Scraping Pipeline
- [x] Create `src/config.py` — paths, URLs, delay settings
- [x] Create `src/utils.py` — logging, file I/O, retry logic
- [x] Create `src/parser.py` — BeautifulSoup parsing for search + tune pages
- [x] Create `src/scraper.py` — HTTP fetching with 15s delay and caching
- [x] Create `src/pipeline.py` — orchestrator with resume/checkpoint
- [x] Clean input CSV from `input_data_w_jayes.md`
- [ ] Test full pipeline on first 3 hymns
- [ ] Run full pipeline on all 190 hymns

### Phase 3: Quarto Website
- [x] Set up `site/_quarto.yml`
- [x] Create `site/styles.scss`
- [x] Create `site/index.qmd` — landing page with summary stats
- [x] Create `site/hymns.qmd` — searchable hymn–tune table
- [x] Create `site/about.qmd` — attribution and methodology
- [ ] Build and test locally with `quarto render`
- [ ] Deploy via GitHub Pages

### Phase 4: Hymn Book Crossover (Pending)
- [ ] Obtain hymn book index from church (title, number, tune name)
- [ ] Build matching logic (by tune name, meter, and/or title)
- [ ] Add crossover results page to Quarto site
- [ ] Highlight matched hymns in the hymns table

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Scraping delay | 15 seconds | 3× robots.txt crawl-delay (5s) |
| HTML caching | Raw files in `data/raw/` | Avoid re-fetching; allows re-parsing |
| A/B variants | Show all tunes, sort by popularity | Let user manually assign A/B later |
| Package manager | uv | Fast, modern, lockfile support |
| Website | Quarto | Integrates Python/R, great for data sites |

## robots.txt Summary (checked 2026-02-07)

- `/tune/` — **Allowed** ✅
- `/text/` — **Allowed** ✅
- `/search?qu=...` — **Allowed** ✅ (only `/search/` with trailing slash is blocked)
- `/result/*` — Blocked for `*` agents (we don't use this path)
- `Crawl-delay: 5` for `*` agents (we use 15s)
