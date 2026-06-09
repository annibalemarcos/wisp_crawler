# WISP — Optimization Mode

This document explains the new **Optimization Mode** introduced to stop
the crawler from saturating CPU, RAM and network on Windows 10 (and any
other host, really).

## TL;DR

* Optimization is **ON by default** after the upgrade.
* Toggle it from the dashboard: **Global Config → Optimization Mode**.
* Or via API: `POST /api/optimization {"optimization_enabled": true}`.

## What it does when enabled

| Resource | Before | After (default) |
|---|---|---|
| Parallel jobs (process-wide) | unlimited | **2** |
| aiohttp total conns per job | 12 | **4** |
| aiohttp per-host conns per job | 4 | **2** |
| Inter-request delay (per domain) | 0.25s | **0.6s** |
| Checkpoint min interval | 2.5s | **10s** |
| Checkpoint queue size | 10 000 | **2 000** |
| Checkpoint `urls_seen` size | 20 000 | **4 000** |
| Checkpoint `dedupe_seen` size | 50 000 | **10 000** |
| Masked outbound cap | up to 500 | **120** |
| Pagination steps cap | 25 | **10** |
| Playwright parallel browsers | unlimited | **1** |
| Playwright Chromium args | default | lightweight (no-sandbox, disable gpu, etc.) |
| Windows process priority | NORMAL | **BELOW_NORMAL** |
| Event bus noisy events | unthrottled | **coalesced (120 ms)** |

The cap on parallel jobs is the single biggest win. Massive Job /
Bulk Run / Config Import can still create 100 jobs at once — they just
queue up and run two at a time, so the machine stays usable.

## How the toggle works

1. The runtime config lives in `data/runtime_config.json` (auto-created).
2. `core/runtime_config.py` is the single source of truth — every other
   module reads from it lazily.
3. Flipping the toggle:
   * Wakes the parallel-job throttle so pending jobs re-evaluate the
     new cap.
   * Re-applies the Windows BELOW_NORMAL priority on Windows hosts.
4. The toggle survives restarts.

## API

```bash
# read current config + defaults
curl http://127.0.0.1:5220/api/optimization

# disable optimization
curl -X POST http://127.0.0.1:5220/api/optimization \
     -H "Content-Type: application/json" \
     -d '{"optimization_enabled": false}'

# tweak a single value while keeping optimization enabled
curl -X POST http://127.0.0.1:5220/api/optimization \
     -H "Content-Type: application/json" \
     -d '{"max_parallel_jobs": 1, "rate_delay_seconds": 1.0}'

# restore defaults
curl -X POST http://127.0.0.1:5220/api/optimization/reset
```

All keys in `runtime_config.DEFAULTS` can be tweaked via the same
endpoint. Unknown keys are ignored silently for forward compatibility.

## Dashboard

Open **Global Config** in the sidebar. The first card is now the
**Optimization Mode** panel: a master switch, a tile per tunable value,
a *Save* button and *Reset to defaults*. Status pill turns green
(`optimized`) or red (`unrestricted`).
