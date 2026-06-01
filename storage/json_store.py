from __future__ import annotations
from pathlib import Path
import json, threading, time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

class JsonStore:
    def __init__(self, subdir: str):
        self.dir = DATA / subdir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()

    def path(self, item_id: str) -> Path:
        return self.dir / f"{item_id}.json"

    def list(self) -> list[dict[str, Any]]:
        with self.lock:
            items = []
            for p in sorted(self.dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
                try:
                    items.append(json.loads(p.read_text(encoding="utf-8")))
                except Exception:
                    continue
            return items

    def get(self, item_id: str) -> dict[str, Any] | None:
        p = self.path(item_id)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def put(self, item_id: str, data: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            data.setdefault("id", item_id)
            data["updated_at"] = data.get("updated_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self.path(item_id).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            return data

    def delete(self, item_id: str) -> bool:
        with self.lock:
            p = self.path(item_id)
            if p.exists():
                p.unlink()
                return True
            return False
