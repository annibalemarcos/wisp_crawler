from __future__ import annotations
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin

NOISE_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid"}

def canonicalize(url: str, base: str | None = None) -> str:
    if base:
        url = urljoin(base, url)
    p = urlparse(url.strip())
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    query = urlencode([(k, v) for k, v in sorted(parse_qsl(p.query, keep_blank_values=True)) if k.lower() not in NOISE_PARAMS])
    return urlunparse((scheme, netloc, path, "", query, ""))

def same_domain(a: str, b: str) -> bool:
    return urlparse(a).netloc.lower().lstrip("www.") == urlparse(b).netloc.lower().lstrip("www.")

def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()

def path_query(url: str) -> str:
    p = urlparse(url)
    return (p.path or "/") + (("?" + p.query) if p.query else "")

def join_url(base: str, candidate: str) -> str:
    return canonicalize(urljoin(base, candidate))
