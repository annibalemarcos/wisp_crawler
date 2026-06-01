from __future__ import annotations
import queue, threading, json, time
from collections import deque
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
EVENTS_FILE = DATA / "events.jsonl"

class EventBus:
    def __init__(self, max_history: int = 10000):
        self.clients: list[queue.Queue] = []
        self.history = deque(maxlen=max_history)
        self.lock = threading.RLock()
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
