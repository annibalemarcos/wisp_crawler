from __future__ import annotations
import queue, threading, json, time
from collections import deque
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
EVENTS_FILE = DATA / "events.jsonl"

# Events that fire very often and are safe to coalesce when optimization mode
# is on. Critical lifecycle events (job_completed, job_failed, job_cancelled,
# job_started, pattern_detected, masked_outbound_resolved, etc.) are never
# throttled.
_THROTTLE_TYPES = {
    "job_progress",
    "node_discovered",
    "edge_created",
    "pagination_step",
    "pagination_links_collected",
    "pattern_applied",
    "url_skipped",
    "masked_outbound_scan",
}


class EventBus:
    def __init__(self, max_history: int = 10000):
        self.clients: list[queue.Queue] = []
        self.history = deque(maxlen=max_history)
        self.lock = threading.RLock()
        # Per-event-type "last published" timestamp used by the throttle.
        self._last_publish: dict[str, float] = {}
        DATA.mkdir(parents=True, exist_ok=True)
        self._load_from_disk()

    def _load_from_disk(self):
        if not EVENTS_FILE.exists():
            return
        try:
            lines = EVENTS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()[-self.history.maxlen:]
            for line in lines:
                if not line.strip():
                    continue
                try:
                    self.history.append(json.loads(line))
                except Exception:
                    continue
        except Exception:
            pass

    def _persist(self, msg: dict):
        try:
            with EVENTS_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def publish(self, event: str, payload: dict | None = None):
        payload = payload or {}
        # High-frequency events are coalesced when the optimization toggle is
        # enabled. We import lazily to avoid an import cycle at module load.
        if event in _THROTTLE_TYPES:
            try:
                from core import runtime_config as _rc
                if _rc.is_optimized():
                    interval = float(_rc.get("event_bus_min_interval_ms") or 0) / 1000.0
                    if interval > 0:
                        now = time.time()
                        last = self._last_publish.get(event, 0.0)
                        if now - last < interval:
                            return
                        self._last_publish[event] = now
            except Exception:
                pass
        msg = {
            "event": event,
            "payload": payload,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ts_unix": time.time(),
        }
        with self.lock:
            self.history.append(msg)
            self._persist(msg)
            for q in list(self.clients):
                try:
                    q.put_nowait(msg)
                except Exception:
                    pass

    def _format(self, msg: dict) -> str:
        data = json.dumps(msg, ensure_ascii=False)
        return f"event: {msg.get('event','message')}\ndata: {data}\n\n"

    def snapshot(self, limit: int = 1000):
        try:
            limit = max(0, int(limit))
        except Exception:
            limit = 1000
        with self.lock:
            # If the process was started before the file existed or history was cleared,
            # opportunistically reload. This makes /api/events reliable after restarts.
            if not self.history and EVENTS_FILE.exists():
                self._load_from_disk()
            return list(self.history)[-limit:]

    def clear_memory_only(self):
        with self.lock:
            self.history.clear()

    def subscribe(self, replay: int = 80, heartbeat_seconds: int = 15):
        q = queue.Queue()
        with self.lock:
            if not self.history and EVENTS_FILE.exists():
                self._load_from_disk()
            self.clients.append(q)
            backlog = list(self.history)[-max(0, int(replay)):]
        try:
            yield ": connected\n\n"
            for msg in backlog:
                yield self._format(msg)
            while True:
                try:
                    msg = q.get(timeout=heartbeat_seconds)
                    yield self._format(msg)
                except queue.Empty:
                    yield f": heartbeat {int(time.time())}\n\n"
        finally:
            with self.lock:
                if q in self.clients:
                    self.clients.remove(q)

event_bus = EventBus()
