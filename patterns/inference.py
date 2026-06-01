from __future__ import annotations
from urllib.parse import urlparse, parse_qsl
from collections import defaultdict
import re
from utils.urls import path_query

COMMON_VALUES = {
    "slug": ["ai", "fintech", "crypto", "health", "openai", "stripe", "binance", "kraken", "coinbase", "okx", "bybit"],
    "category": ["exchange", "swap", "bridge", "dex", "aggregator", "finance", "ai", "health", "crypto", "troca", "intercambio", "agregador"],
    "type": ["exchange", "aggregator", "swap", "bridge", "dex", "troca", "intercambio", "agregador"],
    "page": ["1", "2", "3", "4", "5"],
}

def placeholder_for(value: str, key: str = "") -> str:
    if key: return "query" if key in {"q", "query", "search"} else key
    if value.isdigit(): return "id" if int(value) > 20 else "page"
    if value in COMMON_VALUES["category"]: return "category"
    return "slug"

def infer_templates(urls: list[str]) -> list[dict]:
    parsed = [urlparse(u) for u in urls]
    groups = defaultdict(list)
    for p in parsed:
        segs = [s for s in p.path.split("/") if s]
        groups[(p.netloc, len(segs))].append((p, segs))
    patterns = []
    for (domain, n), items in groups.items():
        if len(items) < 2: continue
        cols = list(zip(*[segs for _, segs in items])) if n else []
        template = []
        vals = []
        var_count = 0
        for col in cols:
            uniq = sorted(set(col))
            if len(uniq) == 1:
                template.append(uniq[0])
            else:
                ph = placeholder_for(uniq[0])
                template.append("{"+ph+"}")
                vals.extend(uniq); var_count += 1
        if var_count:
            confidence = min(0.97, 0.45 + len(items)*0.08 + (0.2 if var_count == 1 else 0))
            patterns.append({"domain": domain, "pattern": "/" + "/".join(template), "type": "path", "confidence": round(confidence,2), "detected_values": sorted(set(vals)), "suggested_values": suggest_values(template, vals), "examples": [path_query(p.geturl()) for p,_ in items[:5]], "enabled": True, "active_values": []})
    # Query patterns
    qgroups = defaultdict(list)
    for p in parsed:
        for k,v in parse_qsl(p.query): qgroups[(p.netloc, p.path or "/", k)].append(v)
    for (domain, path, k), vals in qgroups.items():
        if vals:
            ph = placeholder_for(vals[0], k)
            patt = f"{path}?{k}=" + "{"+ph+"}"
            patterns.append({"domain": domain, "pattern": patt, "type": "query", "confidence": round(min(0.92,0.55+len(set(vals))*0.08),2), "detected_values": sorted(set(vals)), "suggested_values": suggest_values([ph], vals), "examples": [f"{path}?{k}={v}" for v in vals[:5]], "enabled": True, "active_values": []})
    return dedupe_patterns(patterns)

def suggest_values(template, values):
    blob = " ".join(template if isinstance(template, list) else [str(template)]).lower()
    seed = []
    if "categor" in blob or "industr" in blob: seed += COMMON_VALUES["category"]
    if "type" in blob or "categories" in blob: seed += COMMON_VALUES["type"]
    if "slug" in blob or "exchanger" in blob or "compan" in blob: seed += COMMON_VALUES["slug"]
    seed += list(values)
    out=[]
    for v in seed:
        if v and v not in out: out.append(v)
    return out[:30]

def dedupe_patterns(patterns):
    seen=set(); out=[]
    for p in patterns:
        key=(p.get('domain'), p.get('pattern'))
        if key not in seen:
            seen.add(key); out.append(p)
    return out

def build_url(domain: str, pattern: str, value: str) -> str:
    url = pattern
    url = re.sub(r"\{[^}]+\}", value, url, count=1)
    scheme = "https://"
    return scheme + domain + (url if url.startswith('/') else '/' + url)
