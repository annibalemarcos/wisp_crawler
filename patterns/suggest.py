from __future__ import annotations
from patterns.detector import extract_links, detect_patterns

def suggest_from_payload(payload: dict) -> list[dict]:
    urls = list(payload.get("urls") or [])
    base = payload.get("url") or (urls[0] if urls else "")
    html = payload.get("html") or ""
    if base and html:
        urls.extend(extract_links(base, html))
    return detect_patterns(sorted(set(urls)))
