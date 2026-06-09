# WISP — Changes in this round

This round adds the **Optimization Mode** so the crawler stops eating
100% of CPU, RAM and network on Windows 10. The toggle lives in the
**Global Config** view of the dashboard and is **on by default**.

See `OPTIMIZATION.md` for the full table of limits and the API.

## Files added

- `core/runtime_config.py` — single source of truth for every resource
  limit. Persists to `data/runtime_config.json`.
- `OPTIMIZATION.md` — operator-facing docs.

## Files modified

- `core/scheduler.py`
  - Global parallel-job throttle (default cap: 2). Bulk/Massive batches
    queue up cleanly instead of starting N threads at once.
  - `apply_windows_priority_once()` drops the process to BELOW_NORMAL
    on Windows when the toggle is on (no-op on Linux/macOS).
- `core/crawler.py`
  - aiohttp `TCPConnector` honours the optimization caps (default 4
    total / 2 per host instead of 12 / 4).
  - Higher rate-limit floor (0.6s vs 0.25s).
  - Checkpoints save less often and write smaller arrays (≈70 % less
    disk I/O).
  - Playwright is gated by a process-wide semaphore (default: 1 browser
    at a time) and launches with a lightweight Chromium argument set
    (`--no-sandbox`, `--disable-gpu`, `--disable-dev-shm-usage`, etc.).
- `storage/event_bus.py`
  - High-frequency events (`job_progress`, `node_discovered`, …) are
    coalesced when optimization is on; lifecycle events are never
    dropped.
- `api/app.py`
  - `GET /api/optimization` — read current config + defaults.
  - `POST /api/optimization` — toggle or tweak any tunable value.
  - `POST /api/optimization/reset` — restore defaults.
  - Calls `apply_windows_priority_once()` at startup.
- `dashboard/templates/index.html`
  - New **Optimization Mode** card on the Global Config view (master
    switch + tile per tunable).
- `dashboard/static/js/app.js`
  - `loadOptimizationConfig`, `renderOptimizationToggle`,
    `saveOptimizationConfig`, `resetOptimizationConfig` wire the panel
    to the API. Toggle saves immediately on change.
- `dashboard/static/css/app.css`
  - Styles for the optimization card and grid.

## How to run

```bat
install.bat
run_dashboard.bat
```

Then open <http://127.0.0.1:5220>, go to **Global Config** and confirm
the **Optimization Mode** pill says `optimized` (green).

## Validation done

- `GET /api/optimization` returns the persisted config + defaults.
- `POST /api/optimization {"optimization_enabled": false}` flips the
  toggle and persists across restarts.
- Bulk launch of 5 long-running jobs with optimization ON shows only 2
  jobs in `running` state, the other 3 stay `queued` and start as
  slots free up. Same launch with optimization OFF starts all 5
  concurrently — confirming the cap is the only difference.
- Single small job still completes end-to-end (`example.com`, depth 1,
  max_pages 3 → status `completed`, 3 pages).
- Windows priority API is wired but only activates on `os.name == "nt"`.

