"""Central runtime configuration for WISP.

This module stores the *optimization toggle* and all tunable knobs that
control how much CPU / RAM / network the WISP crawler is allowed to use.

The config is persisted under ``data/runtime_config.json`` so the toggle
survives restarts. The dashboard (Global Config view) reads/writes it via
``GET/POST /api/optimization``.

Design goals:
    * One single source of truth for *every* resource limit.
    * Toggleable at runtime without restarting the Flask server.
    * Safe defaults: optimization is **ON by default**, so a fresh install on
      a Windows 10 box does not saturate the machine the first time the user
      clicks Run.
"""
from __future__ import annotations
import json
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "data" / "runtime_config.json"

# Defaults represent the "balanced + safe" profile we recommend for
# everyday Windows 10 use. They are conservative on purpose.
DEFAULTS: dict = {
    # Master switch. When False the crawler runs with the original aggressive
    # settings (which is what caused 100% CPU/RAM on the user's box).
    "optimization_enabled": True,

    # Global cap on the number of jobs that can run concurrently inside the
    # process. Bulk Run / Massive Job / Config Import still create as many
    # jobs as the user asks, but only this many start actual work at once.
    "max_parallel_jobs": 2,

    # aiohttp connector limits per job.
    "aiohttp_total_conns": 4,
    "aiohttp_per_host": 2,

    # Per-domain delay between requests, in seconds.
    "rate_delay_seconds": 0.6,

    # Checkpoint persistence. Higher interval + smaller arrays = less disk
    # thrashing. The original code was saving up to 50000-item JSON blobs
    # every 2.5 seconds, which alone could pin a slow SSD.
    "checkpoint_min_interval": 10.0,
    "checkpoint_max_queue": 2000,
    "checkpoint_max_urls_seen": 4000,
    "checkpoint_max_dedupe": 10000,
    "checkpoint_max_skipped": 1000,

    # SSE event bus throttling. High-frequency events are coalesced so the
    # browser does not melt and the producer does not spend cycles
    # serialising JSON nobody reads.
    "event_bus_min_interval_ms": 120,

    # Caps applied on top of whatever the job payload requests. They never
    # raise the user's limits, only lower them when optimization is on.
    "masked_outbound_limit_cap": 120,
    "pagination_limit_cap": 10,

    # On Windows we drop the process priority to BELOW_NORMAL so the rest of
    # the desktop stays responsive even when the crawler is busy.
    "lower_windows_priority": True,

    # Playwright handling. When optimization is on we launch Chromium with
    # the lightweight argument set and limit the number of jobs that can use
    # Playwright simultaneously.
    "playwright_max_jobs": 1,
    "playwright_lightweight_args": True,
}

_lock = threading.RLock()
_cache: dict | None = None


def _load() -> dict:
    global _cache
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        data = {}
    _cache = {**DEFAULTS, **{k: v for k, v in data.items() if k in DEFAULTS}}
    return _cache


def _save(data: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(CONFIG_PATH)


def get_all() -> dict:
    with _lock:
        if _cache is None:
            _load()
        return dict(_cache)


def get(key: str, default=None):
    with _lock:
        if _cache is None:
            _load()
        if key in _cache:
            return _cache[key]
        return DEFAULTS.get(key, default)


def update(patch: dict) -> dict:
    """Update one or more keys. Unknown keys are silently ignored."""
    with _lock:
        if _cache is None:
            _load()
        changed = False
        for k, v in (patch or {}).items():
            if k not in DEFAULTS:
                continue
            # Coerce numeric strings into numbers when the default is numeric.
            try:
                if isinstance(DEFAULTS[k], bool):
                    if isinstance(v, str):
                        v = v.strip().lower() in {"1", "true", "yes", "on"}
                    else:
                        v = bool(v)
                elif isinstance(DEFAULTS[k], int) and not isinstance(v, bool):
                    v = int(v)
                elif isinstance(DEFAULTS[k], float):
                    v = float(v)
            except Exception:
                continue
            if _cache.get(k) != v:
                _cache[k] = v
                changed = True
        if changed:
            _save(_cache)
        return dict(_cache)


def reset() -> dict:
    with _lock:
        global _cache
        _cache = dict(DEFAULTS)
        _save(_cache)
        return dict(_cache)


def is_optimized() -> bool:
    return bool(get("optimization_enabled", True))
