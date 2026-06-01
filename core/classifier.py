from __future__ import annotations

def classify_site(url: str, html: str = "") -> str:
    u = url.lower(); h = html.lower()[:20000]
    if any(x in u for x in ["/api", "swagger", "openapi"]) or "application/json" in h: return "API-first service"
    if any(x in u for x in ["market", "product", "shop"]): return "marketplace"
    if any(x in u for x in ["docs", "documentation", "guide"]): return "documentation site"
    if any(x in u for x in ["blog", "news", "article"]): return "news site" if "news" in u else "blog"
    if any(x in u for x in ["exchange", "exchanger", "crypto", "swap", "dex"]): return "exchange/crypto service hub"
    if any(x in u for x in ["directory", "companies", "categories", "category", "listing"]): return "directory site"
    return "database/listing site" if len(html) > 4000 else "SaaS"
