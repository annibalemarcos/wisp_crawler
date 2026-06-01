from __future__ import annotations
import re
from bs4 import BeautifulSoup
from utils.urls import join_url
from patterns.inference import infer_templates

ROUTE_RE = re.compile(r"['\"]((?:/|https?://)[A-Za-z0-9_./?=&%#:\-{}+,;~]+)['\"]")
ONCLICK_RE = re.compile(r"(?:location\.href|window\.location|router\.push)\s*\(?\s*['\"]([^'\"]+)")

def extract_links(base_url: str, html: str) -> list[str]:
    links = set()
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup.find_all(True):
        for attr in ["href", "src", "data-href", "data-url", "data-link", "data-target", "data-redirect", "data-original", "data-original-href", "data-destination", "action", "value"]:
            val = tag.get(attr)
            if val and not str(val).startswith(("mailto:", "tel:", "javascript:")):
                links.add(join_url(base_url, str(val)))
        onclick = tag.get("onclick")
        if onclick:
            for m in ONCLICK_RE.findall(onclick): links.add(join_url(base_url, m))
            for m in re.findall(r"(?:https?://[^\s'\"<>]+|/[A-Za-z0-9_./?=&%#:\-{}+,;~]+)", onclick):
                links.add(join_url(base_url, m))
        if tag.name == "meta":
            content = tag.get("content") or ""
            mm = re.search(r"url=([^;]+)", content, flags=re.I)
            if mm: links.add(join_url(base_url, mm.group(1).strip()))
    for m in ROUTE_RE.findall(html or ""):
        if not m.startswith(("#", "mailto:", "tel:")):
            try: links.add(join_url(base_url, m))
            except Exception: pass
    return sorted(links)

def detect_patterns(urls: list[str]) -> list[dict]:
    return infer_templates(urls)
