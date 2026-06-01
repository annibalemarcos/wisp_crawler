from __future__ import annotations
import hashlib
from urllib.parse import urlparse, parse_qsl
from utils.urls import NOISE_PARAMS

def url_fingerprint(url: str) -> str:
    p = urlparse(url)
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    signature = "&".join(sorted(k.lower() for k, _ in parse_qsl(p.query) if k.lower() not in NOISE_PARAMS))
    raw = f"{p.netloc.lower()}|{path.lower()}|{signature}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def stable_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:18]
