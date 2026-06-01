from __future__ import annotations
import aiohttp, asyncio

async def soft_probe(session: aiohttp.ClientSession, url: str, timeout: int = 8) -> dict:
    try:
        async with session.head(url, allow_redirects=True, timeout=timeout) as r:
            if r.status in (405, 403):
                raise RuntimeError("HEAD blocked")
            return {"url": str(r.url), "status": r.status, "valid": 200 <= r.status < 400, "content_type": r.headers.get("content-type", "")}
    except Exception:
        try:
            async with session.get(url, allow_redirects=True, timeout=timeout) as r:
                text = await r.text(errors="ignore")
                empty = len(text.strip()) < 80
                return {"url": str(r.url), "status": r.status, "valid": 200 <= r.status < 400 and not empty, "content_type": r.headers.get("content-type", ""), "empty": empty}
        except Exception as e:
            return {"url": url, "status": 0, "valid": False, "error": str(e)}
