from __future__ import annotations
from storage.pattern_store import seed_store
from utils.hashing import stable_id
import time

def create_seed(data: dict) -> dict:
    sid = data.get("id") or stable_id(data.get("domain",""), data.get("pattern",""), str(time.time()))
    seed = {"id": sid, "domain": data.get("domain",""), "pattern": data.get("pattern",""), "values": data.get("values", []), "language_variants": data.get("language_variants", ["en"]), "enabled": data.get("enabled", True), "priority": data.get("priority", 50), "notes": data.get("notes", ""), "version": data.get("version", 1), "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    return seed_store.put(sid, seed)

def active_seeds(domain: str | None = None) -> list[dict]:
    seeds = [s for s in seed_store.list() if s.get("enabled", True)]
    if domain: seeds = [s for s in seeds if s.get("domain") in (domain, domain.lstrip("www."))]
    return sorted(seeds, key=lambda s: s.get("priority", 50), reverse=True)
