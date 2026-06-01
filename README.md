# WISP — Web Intelligence Scraper Platform

WISP is a Flask-based exploratory web intelligence platform. It crawls sites, mines links, infers URL patterns, generates hypothetical routes, probes them, and builds a live graph.

## Run locally

```bash
pip install -r requirements.txt
python api/app.py
```

Open: http://127.0.0.1:5220

On Windows:

```bat
install.bat
run_dashboard.bat
```

## Core features

- Flask API on fixed port `5220`
- Hybrid crawler: HTTP first, optional Playwright renderer
- Pattern Intelligence Layer (PIL)
- Manual Pattern Seeds
- Clickable Suggested Values
- Re-run jobs with deeper depth/pages
- SSE real-time event stream
- Graph Explorer, Pattern Intelligence, Pattern Seeds, Jobs, Results, Bulk Run
- Hard delete for services, patterns and seeds
- JSON storage abstraction, ready for SQLite/PostgreSQL/Neo4j migration

## API endpoints

All mandatory endpoints are implemented:

- `GET /api/jobs`
- `POST /api/run`
- `GET /api/results`
- `GET /api/stats`
- `GET /api/progress`
- `GET /api/stream`
- `GET /api/graph`
- `POST /api/bulk-run`
- Pattern CRUD
- Pattern Seed CRUD
- Suggested value add/remove
- Pattern suggestion
- Job rerun
- Services list/delete

## Notes

Playwright is optional at runtime. If it is not installed or browsers are unavailable, WISP falls back to `aiohttp`.

## Added intelligence tools

This build keeps the existing aggressive external resolver and adds:

- External Resolver Report: candidates, resolved links, failed masked links and resolver errors.
- Why Not Crawled: skipped URL reasons such as duplicate URL, external domain, max depth, max pages, soft probe invalid and fetch errors.
- Pattern Lab: preview a URL pattern, soft-probe generated URLs and save the template as a seed.
- Extra exports: CSV, JSON, Markdown, GraphML and GEXF in addition to HTML/TXT/PDF where relevant.
- Auto-resume notice for running jobs in the Live Events panel.
