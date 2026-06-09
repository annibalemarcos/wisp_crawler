from __future__ import annotations
import asyncio, time, heapq, re
from urllib.parse import urlparse, parse_qsl, unquote
import aiohttp
from bs4 import BeautifulSoup
try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None
from core.dedupe import Dedupe
from utils.hashing import url_fingerprint
from storage.job_store import job_store
from storage.checkpoint_store import checkpoint_store
from core.classifier import classify_site
from patterns.detector import extract_links, detect_patterns
from patterns.inference import build_url
from patterns.seeds import active_seeds
from patterns.validators import soft_probe
from graph.graph_builder import GraphBuilder
from utils.urls import same_domain, domain_of, canonicalize
from utils.rate_limit import DomainRateLimiter
from storage.event_bus import event_bus
from storage.blacklist_store import list_rules as list_blacklist_rules, explain_match as blacklist_explain_match
from core import runtime_config

# When optimization is on, only a handful of CrawlEngine instances are allowed
# to spin up Playwright at once. Each successful acquire must be balanced by a
# release in the engine's ``finally`` block.
_PLAYWRIGHT_SEM = asyncio.Lock()  # placeholder; real semaphore is built per loop below
import threading as _threading
_PW_SLOT_LOCK = _threading.Condition()
_PW_SLOT_BUSY = 0


def _acquire_playwright_slot(timeout: float = 60.0) -> bool:
    """Cross-thread cap on simultaneous Playwright browsers."""
    global _PW_SLOT_BUSY
    deadline = time.time() + max(1.0, timeout)
    with _PW_SLOT_LOCK:
        while True:
            cap = max(1, int(runtime_config.get("playwright_max_jobs") or 1)) if runtime_config.is_optimized() else 10 ** 6
            if _PW_SLOT_BUSY < cap:
                _PW_SLOT_BUSY += 1
                return True
            if time.time() >= deadline:
                return False
            _PW_SLOT_LOCK.wait(timeout=min(2.0, max(0.1, deadline - time.time())))


def _release_playwright_slot() -> None:
    global _PW_SLOT_BUSY
    with _PW_SLOT_LOCK:
        _PW_SLOT_BUSY = max(0, _PW_SLOT_BUSY - 1)
        _PW_SLOT_LOCK.notify_all()

COMMON_ROUTES = ["/api/", "/api/v1/", "/api/search", "/api/directory", "/api/explore", "/api/categories", "/search", "/directory", "/explore", "/categories", "/market", "/markets", "/services", "/products", "/sitemap.xml", "/robots.txt"]

class JobCancelled(Exception):
    pass

class JobRestartRequested(Exception):
    pass


class CrawlEngine:
    def __init__(self, job: dict):
        self.job = job
        self.depth = int(job.get("depth", 2))
        self.max_pages = int(job.get("max_pages", 200))
        self.root = canonicalize(job["url"])
        self.domain = domain_of(self.root)
        self.graph = GraphBuilder()
        self.dedupe = Dedupe()
        self.urls_seen = []
        self.patterns = []
        # Optimization: bump the inter-request delay if the runtime config asks
        # for it. The user's per-job ``rate_delay`` is honored if it's larger.
        _opt = runtime_config.is_optimized()
        raw_delay = float(job.get("rate_delay", 0.25))
        if _opt:
            raw_delay = max(raw_delay, float(runtime_config.get("rate_delay_seconds") or 0.6))
        self.rate = DomainRateLimiter(raw_delay)
        self.use_playwright = bool(job.get("playwright", False)) and async_playwright is not None
        self.options = job.get("options", {})
        self.use_blacklist_dirs = bool(self.options.get("use_blacklist_dirs", True))
        self.blacklist_rules = []
        if self.use_blacklist_dirs:
            try:
                self.blacklist_rules = list_blacklist_rules()
            except Exception:
                self.blacklist_rules = []
            # Per-job temporary rules, useful for Massive Job experiments.
            for row in self.options.get("blacklist_extra", []) or []:
                if isinstance(row, str):
                    self.blacklist_rules.append({"pattern": row, "kind": "contains", "enabled": True, "scope": "job"})
                elif isinstance(row, dict):
                    self.blacklist_rules.append({**row, "enabled": row.get("enabled", True)})
        self.follow_masked_outbound = bool(self.options.get("follow_masked_outbound", False))
        self.masked_outbound_aggressive = bool(self.options.get("masked_outbound_aggressive", False))
        _default_limit = 500 if self.masked_outbound_aggressive else 120
        self.masked_outbound_limit = int(self.options.get("masked_outbound_limit", _default_limit))
        if _opt:
            self.masked_outbound_limit = min(self.masked_outbound_limit, int(runtime_config.get("masked_outbound_limit_cap") or 120))
        self.masked_outbound_seen = 0
        self.masked_detail_boost = bool(self.options.get("masked_detail_boost", self.masked_outbound_aggressive))
        self.external_resolver_timeout = int(self.options.get("external_resolver_timeout", 22 if self.masked_outbound_aggressive else 15))
        self.external_theme_filter = bool(self.options.get("external_theme_filter", False))
        self.external_theme_keywords = self._split_theme_keywords(self.options.get("external_theme_keywords", []))
        self.auto_pagination = bool(self.options.get("auto_pagination", False))
        self.pagination_limit = int(self.options.get("pagination_limit", 25))
        if _opt:
            self.pagination_limit = min(self.pagination_limit, int(runtime_config.get("pagination_limit_cap") or 10))
        self.pagination_seen = set()
        self.pagination_report = {"pages": [], "links_found": 0, "errors": []}
        self.skipped = []
        self.external_report = {"candidates": [], "resolved": [], "failed": [], "errors": []}
        self.started_at = time.time()
        self.last_checkpoint_at = 0.0
        self.pages_done = 0
        self.resume_from_checkpoint = bool(job.get("resume_from_checkpoint"))
        # Track whether we hold a Playwright slot so we release exactly once.
        self._playwright_slot_held = False

    def record_skip(self, url: str, reason: str, **meta):
        item = {"url": url, "reason": reason, **meta}
        if len(self.skipped) < 3000:
            self.skipped.append(item)
        event_bus.publish("url_skipped", {"job_id": self.job.get("id"), **item})

    def record_external(self, bucket: str, item: dict):
        if bucket in self.external_report and len(self.external_report[bucket]) < 5000:
            self.external_report[bucket].append(item)

    THEME_STOPWORDS = {
        "http", "https", "www", "com", "org", "net", "html", "htm", "index", "home", "new", "app", "page", "pages",
        "site", "website", "directory", "directories", "company", "companies", "service", "services", "provider", "providers",
        "about", "contact", "login", "signin", "signup", "terms", "privacy", "cookie", "cookies", "blog", "news", "press",
        "and", "or", "the", "for", "with", "from", "your", "you", "our", "are", "was", "were", "this", "that", "have", "has"
    }

    def _split_theme_keywords(self, raw) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple, set)):
            parts = []
            for item in raw:
                parts.extend(self._split_theme_keywords(item))
            return list(dict.fromkeys(parts))[:80]
        text = str(raw)
        parts = re.split(r"[,;\n\r\t|]+", text)
        out = []
        for p in parts:
            k = re.sub(r"[^a-z0-9+#.\- ]+", " ", p.lower()).strip()
            if not k:
                continue
            # Keep explicit multi-word phrases, but also normalize accidental double spaces.
            k = re.sub(r"\s+", " ", k)
            if len(k) >= 3 and k not in self.THEME_STOPWORDS and k not in out:
                out.append(k)
        return out[:80]

    def _theme_tokens_from_text(self, text: str, limit: int = 80) -> list[str]:
        text = re.sub(r"https?://\S+", " ", str(text or "").lower())
        words = re.findall(r"[a-z][a-z0-9+#.\-]{2,}", text)
        out = []
        for w in words:
            w = w.strip(".-_")
            if len(w) < 3 or w in self.THEME_STOPWORDS or w.isdigit():
                continue
            if w not in out:
                out.append(w)
            if len(out) >= limit:
                break
        return out

    def _meta_text_from_html(self, html: str) -> str:
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html[:250000], "html.parser")
            chunks = []
            if soup.title and soup.title.string:
                chunks.append(soup.title.string)
            for tag in soup.find_all("meta"):
                key = (tag.get("name") or tag.get("property") or "").lower()
                if key in {"description", "keywords", "og:title", "og:description", "twitter:title", "twitter:description"}:
                    chunks.append(tag.get("content") or "")
            return " ".join(chunks)
        except Exception:
            return ""

    def _auto_theme_keywords(self, parent_url: str = "", parent_html: str = "") -> list[str]:
        seeds = []
        seeds.extend(self.external_theme_keywords)
        seeds.extend(self._theme_tokens_from_text(self.root))
        seeds.extend(self._theme_tokens_from_text(parent_url))
        seeds.extend(self._theme_tokens_from_text(self._meta_text_from_html(parent_html), limit=80))
        return list(dict.fromkeys([x for x in seeds if x and x not in self.THEME_STOPWORDS]))[:80]

    async def _fetch_external_meta(self, session, target_url: str, referer: str = "") -> dict:
        try:
            p = urlparse(target_url)
            if not p.scheme or not p.netloc:
                return {"ok": False, "error": "invalid_external_url"}
            homepage = f"{p.scheme}://{p.netloc}/"
            headers = {"User-Agent": "Mozilla/5.0 WISP External Theme Filter", "Accept": "text/html,application/xhtml+xml"}
            if referer:
                headers["Referer"] = referer
            # Meta check should be cheap: homepage first because the export is domain-oriented.
            for u in list(dict.fromkeys([homepage, target_url])):
                try:
                    await self.rate.wait(domain_of(u))
                    async with session.get(u, timeout=min(max(5, self.external_resolver_timeout), 15), allow_redirects=True, headers=headers) as r:
                        ctype = r.headers.get("content-type", "")
                        if r.status >= 400 or "html" not in ctype.lower():
                            continue
                        html = await r.text(errors="ignore")
                        meta_text = self._meta_text_from_html(html)
                        return {"ok": True, "url": str(r.url), "status": r.status, "content_type": ctype, "meta_text": meta_text[:2000]}
                except Exception:
                    continue
            return {"ok": False, "error": "meta_unavailable"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def _external_matches_theme(self, session, target_url: str, parent_url: str, parent_html: str) -> dict:
        if not self.external_theme_filter:
            return {"ok": True, "reason": "filter_off", "keywords": []}
        keywords = self._auto_theme_keywords(parent_url, parent_html)
        if not keywords:
            # Sem palavra-chave, não bloqueia tudo no escuro. Máquina burra com confiança alta é incêndio.
            return {"ok": True, "reason": "no_theme_keywords", "keywords": []}
        meta = await self._fetch_external_meta(session, target_url, referer=parent_url)
        if not meta.get("ok"):
            return {"ok": False, "reason": meta.get("error") or "meta_unavailable", "keywords": keywords, "meta": meta}
        hay = (meta.get("meta_text") or "").lower()
        matched = []
        for k in keywords:
            kk = str(k).lower().strip()
            if not kk:
                continue
            if " " in kk:
                if kk in hay:
                    matched.append(k)
            elif re.search(rf"(?<![a-z0-9]){re.escape(kk)}(?![a-z0-9])", hay):
                matched.append(k)
        return {"ok": bool(matched), "reason": "matched" if matched else "theme_mismatch", "keywords": keywords, "matched": matched[:20], "meta": meta}

    def _now_iso(self):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _touch_job(self, **updates):
        jid = self.job.get("id")
        if not jid:
            return
        current = job_store.get(jid) or self.job
        current.update(updates)
        current["last_heartbeat"] = self._now_iso()
        current["last_heartbeat_unix"] = time.time()
        # elapsed_seconds excludes paused time only approximately; good enough for operator visibility.
        started = current.get("started_unix") or self.job.get("started_unix") or self.started_at
        paused_total = float(current.get("paused_total_seconds") or 0)
        if current.get("paused_at_unix") and current.get("status") == "paused":
            paused_total += max(0, time.time() - float(current.get("paused_at_unix") or time.time()))
        current["elapsed_seconds"] = int(max(0, time.time() - float(started) - paused_total))
        job_store.put(jid, current)
        self.job.update(current)

    async def _control_gate(self, url: str | None = None):
        """Cooperative job control: pause/resume/cancel/restart.

        This is checked before page fetches and redirect resolution. If a request is already
        inside an HTTP/Playwright timeout, control takes effect on the next check.
        """
        jid = self.job.get("id")
        if not jid:
            return
        while True:
            current = job_store.get(jid) or self.job
            control = (current.get("control") or "").lower()
            if control in ("cancel", "stop", "delete") or current.get("status") in ("cancel_requested", "deleting"):
                event_bus.publish("job_cancelled", {"job_id": jid, "url": url, "reason": "operator_request"})
                raise JobCancelled("cancelled by operator")
            if control in ("restart", "restart_requested"):
                event_bus.publish("job_restart_requested", {"job_id": jid, "url": url})
                raise JobRestartRequested("restart requested by operator")
            if control in ("pause", "paused") or current.get("status") == "pause_requested":
                if current.get("status") != "paused":
                    current["status"] = "paused"
                    current["control"] = "paused"
                    current["paused_at"] = self._now_iso()
                    current["paused_at_unix"] = time.time()
                    job_store.put(jid, current)
                    event_bus.publish("job_paused", {"job_id": jid, "url": url, "pages": self.pages_done})
                await asyncio.sleep(0.5)
                continue
            if control in ("resume", "running") and current.get("status") in ("paused", "resuming"):
                paused_at = float(current.get("paused_at_unix") or time.time())
                current["paused_total_seconds"] = float(current.get("paused_total_seconds") or 0) + max(0, time.time() - paused_at)
                current["paused_at_unix"] = None
                current["paused_at"] = None
                current["status"] = "running"
                current["control"] = "running"
                job_store.put(jid, current)
                event_bus.publish("job_resumed", {"job_id": jid, "url": url, "pages": self.pages_done})
                self.job.update(current)
            return

    def _load_checkpoint(self):
        if not self.resume_from_checkpoint:
            return None
        cp = checkpoint_store.get(self.job.get("id"))
        if not cp:
            return None
        try:
            self.urls_seen = cp.get("urls_seen", []) or []
            self.patterns = cp.get("patterns", []) or []
            self.skipped = cp.get("skipped", []) or []
            self.external_report = cp.get("external_report", self.external_report) or self.external_report
            self.pagination_report = cp.get("pagination_report", self.pagination_report) or self.pagination_report
            self.masked_outbound_seen = int(cp.get("masked_outbound_seen", 0) or 0)
            g = cp.get("graph") or {}
            self.graph.nodes = {n.get("id"): n for n in g.get("nodes", []) if n.get("id")}
            self.graph.edges = {e.get("id"): e for e in g.get("edges", []) if e.get("id")}
            for fp in cp.get("dedupe_seen", []) or []:
                self.dedupe.seen.add(fp)
            return cp
        except Exception as e:
            event_bus.publish("checkpoint_load_failed", {"job_id": self.job.get("id"), "error": str(e)})
            return None

    def _save_checkpoint(self, pq, pages: int, current_url: str | None = None, force: bool = False):
        now = time.time()
        # Optimization keeps the disk quiet by widening the minimum interval
        # between checkpoints. The user can still force a save (e.g. on errors).
        _opt = runtime_config.is_optimized()
        min_interval = float(runtime_config.get("checkpoint_min_interval") or 10.0) if _opt else 2.5
        if not force and now - self.last_checkpoint_at < min_interval:
            return
        self.last_checkpoint_at = now
        queue_rows = []
        max_queue = int(runtime_config.get("checkpoint_max_queue") or 2000) if _opt else 10000
        max_urls_seen = int(runtime_config.get("checkpoint_max_urls_seen") or 4000) if _opt else 20000
        max_dedupe = int(runtime_config.get("checkpoint_max_dedupe") or 10000) if _opt else 50000
        max_skipped = int(runtime_config.get("checkpoint_max_skipped") or 1000) if _opt else 3000
        try:
            # Store enough queue to restart sensibly without creating giant JSON files.
            for prio, depth, url, source, parent in sorted(list(pq))[:max_queue]:
                queue_rows.append({"priority": prio, "depth": depth, "url": url, "source": source, "parent": parent})
        except Exception:
            queue_rows = []
        cp = {
            "id": self.job.get("id"),
            "job_id": self.job.get("id"),
            "url": self.root,
            "saved_at": self._now_iso(),
            "saved_unix": now,
            "pages": pages,
            "current_url": current_url,
            "queue": queue_rows,
            "queue_size": len(queue_rows),
            "urls_seen": self.urls_seen[-max_urls_seen:],
            "dedupe_seen": list(self.dedupe.seen)[-max_dedupe:],
            "graph": self.graph.as_dict(),
            "patterns": self.patterns[-1000:],
            "skipped": self.skipped[-max_skipped:],
            "external_report": self.external_report,
            "pagination_report": self.pagination_report,
            "masked_outbound_seen": self.masked_outbound_seen,
        }
        checkpoint_store.put(self.job.get("id"), cp)
        self._touch_job(checkpoint={"pages": pages, "queue": len(queue_rows), "saved_at": cp["saved_at"], "current_url": current_url}, progress={"pages": pages, "queued": len(queue_rows), "current_url": current_url, "external_resolved": len(self.external_report.get("resolved", [])), "external_failed": len(self.external_report.get("failed", [])), "skipped": len(self.skipped)})

    async def fetch_http(self, session, url):
        await self.rate.wait(domain_of(url))
        try:
            async with session.get(url, timeout=12, allow_redirects=True, headers={"User-Agent":"WISP/1.0 exploratory crawler"}) as r:
                ct = r.headers.get("content-type", "")
                text = await r.text(errors="ignore") if "text" in ct or "html" in ct or "json" in ct else ""
                return {"url": str(r.url), "status": r.status, "html": text, "content_type": ct}
        except Exception as e:
            return {"url": url, "status": 0, "html": "", "content_type": "", "error": str(e)}

    def is_masked_outbound_candidate(self, url: str) -> bool:
        """Return True for same-domain redirect/masking links.

        Aggressive mode deliberately treats more same-domain redirect-shaped URLs as candidates,
        including OKchanger-style /away?sType=...&sId=... links and generic tracking parameters.
        """
        if not url or not same_domain(self.root, url):
            return False
        p = urlparse(url)
        path = (p.path or "").lower()
        query = (p.query or "").lower()
        hay = f"{path}?{query}"
        redirect_paths = (
            "/away", "/go", "/out", "/external", "/redirect", "/visit", "/jump",
            "/track", "/click", "/link", "/url", "/exit", "/forward", "/partner"
        )
        redirect_params = (
            "url=", "u=", "uri=", "target=", "to=", "link=", "href=", "dest=", "destination=",
            "redirect=", "redirect_url=", "returnurl=", "surl=", "r=", "q=", "surl=", "sid=", "sid=", "sid=",
            "stype=", "sid="
        )
        if any(path.startswith(x) or x in path for x in redirect_paths):
            return True
        if any(x in query for x in redirect_params):
            return True
        if self.masked_outbound_aggressive and any(x in hay for x in ("away?", "?stype=", "&sid=", "serviceid=", "merchantid=")):
            return True
        return False

    def _external_from_text(self, text: str):
        """Find a likely external URL hidden inside redirect HTML/JS/text."""
        if not text:
            return None
        text = unquote(str(text))
        # Common JS/meta forms: location.href='...', location.replace('...'), url=https://...
        patterns = [
            r"(?:location(?:\.href)?|window\.location|document\.location|location\.replace|location\.assign)\s*(?:=|\()\s*['\"]([^'\"]+)",
            r"<meta[^>]+http-equiv=['\"]?refresh['\"]?[^>]+content=['\"][^'\"]*url=([^'\";>]+)",
            r"(?:url|u|target|to|link|href|dest|destination|redirect_url)=([^&\s'\"<>]+)",
            r"(https?://[^\s'\"<>\\)]+)",
        ]
        for pat in patterns:
            for m in re.findall(pat, text, flags=re.I):
                cand = canonicalize(m.strip())
                if cand and cand.startswith(("http://", "https://")) and not same_domain(self.root, cand):
                    return cand
        return None

    def extract_masked_candidates_from_html(self, base_url: str, html: str) -> list[str]:
        """Pull redirect candidates from all attributes and raw HTML, not just href/src."""
        found = set()
        if not html:
            return []
        soup = BeautifulSoup(html or "", "html.parser")
        rich_attrs = [
            "href", "src", "data-href", "data-url", "data-link", "data-target", "data-redirect",
            "data-original", "data-original-href", "data-destination", "data-away", "onclick", "action", "value"
        ]
        for tag in soup.find_all(True):
            for attr in rich_attrs:
                val = tag.get(attr)
                if not val:
                    continue
                val = str(val)
                for candidate in re.findall(r"(?:https?://[^\s'\"<>]+|/[A-Za-z0-9_./?=&%#:\-{}]+)", val):
                    try:
                        c = canonicalize(candidate, base_url)
                        if self.is_masked_outbound_candidate(c):
                            found.add(c)
                    except Exception:
                        pass
        # Raw HTML catches encoded strings in scripts/tables.
        for candidate in re.findall(r"(?:https?://[^\s'\"<>]+|/[A-Za-z0-9_./?=&%#:\-{}]+)", html or ""):
            try:
                c = canonicalize(unquote(candidate), base_url)
                if self.is_masked_outbound_candidate(c):
                    found.add(c)
            except Exception:
                pass
        return sorted(found)

    async def resolve_outbound_target(self, session, url, browser=None, referer=None):
        """Resolve a masked/tracking link and return the final external URL.

        Strategy:
        1) normal GET with redirects, cookies and Referer;
        2) parse returned HTML/JS/meta refresh for hidden destinations;
        3) in aggressive mode, use Playwright as a fallback and wait for navigation.
        """
        await self._control_gate(url)
        if self.masked_outbound_seen >= self.masked_outbound_limit:
            return None
        self.masked_outbound_seen += 1
        await self.rate.wait(domain_of(url))
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 WISP/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": referer or self.root,
        }
        try:
            async with session.get(url, timeout=self.external_resolver_timeout, allow_redirects=True, headers=headers) as r:
                ct = r.headers.get("content-type", "")
                final_url = canonicalize(str(r.url))
                if final_url and not same_domain(self.root, final_url):
                    return {"url": final_url, "status": r.status, "content_type": ct, "masked_url": url, "method": "http_redirect"}
                # Not all masked links issue HTTP 3xx. Some return HTML/JS/meta refresh.
                body = await r.text(errors="ignore") if "text" in ct or "html" in ct or not ct else ""
                hidden = self._external_from_text(body)
                if hidden:
                    return {"url": hidden, "status": r.status, "content_type": ct or "text/html", "masked_url": url, "method": "html_or_js"}
                # Some redirectors encode the destination as query param.
                for _, v in parse_qsl(urlparse(url).query, keep_blank_values=True):
                    hidden = self._external_from_text(v)
                    if hidden:
                        return {"url": hidden, "status": r.status, "content_type": ct, "masked_url": url, "method": "query_param"}
        except Exception as e:
            self.record_external("errors", {"url": url, "error": str(e), "stage": "http"})
            event_bus.publish("masked_outbound_error", {"url": url, "error": str(e), "stage": "http"})
        if self.masked_outbound_aggressive and browser:
            try:
                page = await browser.new_page(user_agent="Mozilla/5.0 WISP aggressive outbound resolver")
                await page.goto(url, wait_until="domcontentloaded", timeout=self.external_resolver_timeout * 1000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass
                final_url = canonicalize(page.url)
                html = await page.content()
                await page.close()
                if final_url and not same_domain(self.root, final_url):
                    return {"url": final_url, "status": 200, "content_type": "text/html; playwright", "masked_url": url, "method": "browser_navigation"}
                hidden = self._external_from_text(html)
                if hidden:
                    return {"url": hidden, "status": 200, "content_type": "text/html; playwright", "masked_url": url, "method": "browser_html"}
            except Exception as e:
                self.record_external("errors", {"url": url, "error": str(e), "stage": "browser"})
                event_bus.publish("masked_outbound_error", {"url": url, "error": str(e), "stage": "browser"})
        return None

    async def collect_masked_outbound(self, session, links, parent_url, depth, html="", browser=None):
        if not self.follow_masked_outbound:
            return set()
        candidates = set()
        for l in links or []:
            try:
                c = canonicalize(l)
                if self.is_masked_outbound_candidate(c):
                    candidates.add(c)
            except Exception:
                pass
        if self.masked_outbound_aggressive:
            candidates.update(self.extract_masked_candidates_from_html(parent_url, html or ""))
        resolved_count = 0
        for cand in sorted(candidates):
            self.record_external("candidates", {"url": cand, "from": parent_url, "depth": depth})
        for l in list(dict.fromkeys(sorted(candidates))):
            await self._control_gate(l)
            if self.masked_outbound_seen >= self.masked_outbound_limit:
                self.record_skip(l, "external_resolver_limit", from_url=parent_url, limit=self.masked_outbound_limit)
                self.record_external("failed", {"url": l, "from": parent_url, "reason": "external_resolver_limit"})
                break
            resolved = await self.resolve_outbound_target(session, l, browser=browser, referer=parent_url)
            if not resolved:
                self.record_external("failed", {"url": l, "from": parent_url, "reason": "not_resolved"})
                self.record_skip(l, "masked_external_not_resolved", from_url=parent_url)
                continue
            target = resolved["url"]
            theme_check = await self._external_matches_theme(session, target, parent_url, html or "")
            if not theme_check.get("ok"):
                self.record_skip(target, "external_theme_mismatch", from_url=parent_url, reason=theme_check.get("reason"), matched=theme_check.get("matched", []), keywords=(theme_check.get("keywords") or [])[:20])
                self.record_external("failed", {"url": target, "from": parent_url, "reason": "external_theme_mismatch", "theme_reason": theme_check.get("reason"), "keywords": (theme_check.get("keywords") or [])[:20], "meta_url": (theme_check.get("meta") or {}).get("url")})
                event_bus.publish("external_theme_filtered", {"job_id": self.job.get("id"), "from": parent_url, "target": target, "reason": theme_check.get("reason"), "keywords": (theme_check.get("keywords") or [])[:20]})
                continue
            if not self.dedupe.add(target):
                continue
            resolved_count += 1
            self.urls_seen.append(target)
            node = self.graph.add_node(
                target,
                depth=depth+1,
                type="external_service",
                source="external",
                status_code=resolved.get("status", 0),
                confidence=0.98 if self.masked_outbound_aggressive else 0.96,
                parent=parent_url,
                masked_from=resolved.get("masked_url"),
                resolver_method=resolved.get("method", "redirect"),
                content_type=resolved.get("content_type", ""),
                theme_matched_keywords=theme_check.get("matched", []),
                theme_filter=bool(self.external_theme_filter)
            )
            self.record_external("resolved", {"from": parent_url, "masked_url": resolved.get("masked_url"), "target": target, "status": resolved.get("status", 0), "method": resolved.get("method"), "theme_matched_keywords": theme_check.get("matched", []), "theme_filter": bool(self.external_theme_filter)})
            event_bus.publish("masked_outbound_resolved", {"from": parent_url, "masked_url": resolved.get("masked_url"), "target": target, "status": resolved.get("status", 0), "method": resolved.get("method")})
            event_bus.publish("node_discovered", node)
            edge = self.graph.add_edge(parent_url, target, source="external", relationship="masked_redirect")
            event_bus.publish("edge_created", edge)
        if candidates:
            event_bus.publish("masked_outbound_scan", {"from": parent_url, "candidates": len(candidates), "resolved": resolved_count, "limit": self.masked_outbound_limit, "seen": self.masked_outbound_seen})
        return candidates


    def looks_paginated_html(self, html: str) -> bool:
        if not html:
            return False
        h = html.lower()[:250000]
        return any(x in h for x in ("datatable", "paginate", "pagination", "next", "show", "rows", "<table", "rel=\"next", "rel='next"))

    def extract_pagination_links_from_html(self, base_url: str, html: str) -> list[str]:
        """Find explicit pagination links in the static HTML: rel=next, ?page=2, /page/2, next buttons."""
        if not self.auto_pagination or not html:
            return []
        found = set()
        soup = BeautifulSoup(html or "", "html.parser")
        next_words = {"next", "next ›", "next >", ">", "›", "»", "próximo", "proximo", "seguinte", "more", "load more"}
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            rel = " ".join(a.get("rel") or []).lower()
            txt = " ".join(a.get_text(" ", strip=True).lower().split())
            cls = " ".join(a.get("class") or []).lower()
            aria = (a.get("aria-label") or "").lower()
            candidate = canonicalize(href, base_url)
            if not same_domain(self.root, candidate):
                continue
            parsed = urlparse(candidate)
            hay = f"{parsed.path}?{parsed.query}".lower()
            is_next = "next" in rel or txt in next_words or "next" in cls or "next" in aria
            is_pageish = bool(re.search(r"(?:[?&](?:page|p|start|offset|draw)=\d+|/page/\d+|/p/\d+)", hay))
            if is_next or is_pageish:
                found.add(candidate)
        return sorted(found)

    async def extract_paginated_links_with_browser(self, browser, url: str) -> list[str]:
        """Use a real browser to walk client-side pagination/DataTables and collect visible anchors.

        This does not crawl external domains. It only asks the current directory/listing page to reveal
        more rows, captures links, and then the normal crawler queue decides what to crawl/resolve.
        """
        if not self.auto_pagination or not browser or self.pagination_limit <= 0:
            return []
        await self._control_gate(url)
        key = canonicalize(url)
        if key in self.pagination_seen:
            return []
        self.pagination_seen.add(key)
        page = None
        collected = set()
        snapshots = set()
        steps = 0
        try:
            page = await browser.new_page(user_agent="Mozilla/5.0 WISP pagination walker")
            await page.goto(url, wait_until="domcontentloaded", timeout=18000)
            try:
                await page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass

            async def collect(stage: str):
                nonlocal collected
                hrefs = await page.evaluate("""() => Array.from(document.querySelectorAll('a[href]')).map(a => a.href).filter(Boolean)""")
                before = len(collected)
                for h in hrefs or []:
                    try:
                        c = canonicalize(h)
                        if same_domain(self.root, c) or (self.follow_masked_outbound and self.is_masked_outbound_candidate(c)):
                            collected.add(c)
                    except Exception:
                        pass
                added = len(collected) - before
                self.pagination_report["links_found"] = int(self.pagination_report.get("links_found", 0)) + max(0, added)
                return added

            await collect("initial")

            # DataTables and similar widgets often hide extra rows behind a length select.
            # Choose the largest available page size; if -1 exists, that usually means "All".
            try:
                selects = await page.query_selector_all("select")
                for sel in selects[:8]:
                    opts = await sel.evaluate("""s => Array.from(s.options).map(o => ({value:o.value, text:o.textContent || ''}))""")
                    numeric = []
                    for o in opts or []:
                        raw = str(o.get("value") or o.get("text") or "").strip()
                        try:
                            val = int(re.sub(r"\D+", "", raw) or raw)
                        except Exception:
                            continue
                        numeric.append((val, str(o.get("value"))))
                    if numeric:
                        chosen = sorted(numeric, key=lambda x: (x[0] == -1, x[0]))[-1][1]
                        try:
                            await sel.select_option(chosen)
                            await page.wait_for_timeout(800)
                            await collect("page_length")
                        except Exception:
                            pass
            except Exception as e:
                self.pagination_report["errors"].append({"url": url, "stage": "length_select", "error": str(e)})

            while steps < self.pagination_limit:
                await self._control_gate(url)
                # Snapshot visible table/link text to stop loops.
                sig = await page.evaluate("""() => Array.from(document.querySelectorAll('table tr, a[href]')).slice(0,80).map(x => (x.innerText || x.href || '').trim()).join('|').slice(0,3000)""")
                if sig in snapshots and steps > 0:
                    break
                snapshots.add(sig)

                clicked = await page.evaluate("""() => {
                  const bad = el => !el || el.disabled || el.getAttribute('aria-disabled') === 'true' || /disabled/.test(el.className || '');
                  const textOf = el => ((el.innerText || el.textContent || el.getAttribute('aria-label') || el.title || '') + '').trim().toLowerCase();
                  const candidates = Array.from(document.querySelectorAll('a,button,[role=button],.paginate_button'));
                  const next = candidates.find(el => !bad(el) && (/\bnext\b|próximo|proximo|seguinte|›|»|^>$/.test(textOf(el)) || /\bnext\b/.test(el.className || '')));
                  if (!next) return false;
                  next.scrollIntoView({block:'center', inline:'center'});
                  next.click();
                  return true;
                }""")
                if not clicked:
                    break
                steps += 1
                try:
                    await page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    await page.wait_for_timeout(900)
                added = await collect(f"next_{steps}")
                event_bus.publish("pagination_step", {"job_id": self.job.get("id"), "url": url, "step": steps, "added_links": added, "total_links": len(collected)})
                # If several clicks add nothing, stop. Many pages keep a disabled next-looking control around.
                if added == 0 and steps >= 2:
                    break

            self.pagination_report["pages"].append({"url": url, "steps": steps, "links_found": len(collected)})
            if collected:
                event_bus.publish("pagination_scan", {"job_id": self.job.get("id"), "url": url, "steps": steps, "links_found": len(collected)})
            return sorted(collected)
        except Exception as e:
            self.pagination_report["errors"].append({"url": url, "stage": "browser_pagination", "error": str(e)})
            event_bus.publish("pagination_error", {"job_id": self.job.get("id"), "url": url, "error": str(e)})
            return []
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass

    async def fetch_playwright(self, browser, url):
        try:
            page = await browser.new_page(user_agent="WISP/1.0 exploratory crawler")
            await page.goto(url, wait_until="networkidle", timeout=18000)
            html = await page.content()
            final_url = page.url
            await page.close()
            return {"url": final_url, "status": 200, "html": html, "content_type": "text/html; playwright"}
        except Exception as e:
            return {"url": url, "status": 0, "html": "", "content_type": "", "error": str(e)}

    def blacklist_match(self, url: str, depth: int = 0):
        """Return blacklist match metadata, but never block the exact root URL.

        Depth-0/root is allowed so a user can intentionally start inside a known directory
        even if a broad rule would otherwise match that path.
        """
        if not self.use_blacklist_dirs:
            return None
        try:
            if canonicalize(url) == canonicalize(self.root):
                return None
        except Exception:
            pass
        return blacklist_explain_match(url, self.blacklist_rules)

    def should_skip_blacklisted(self, url: str, depth: int = 0, from_url: str = "", source: str = "") -> bool:
        hit = self.blacklist_match(url, depth)
        if not hit:
            return False
        self.record_skip(url, "blacklisted_dir", from_url=from_url, depth=depth, source=source, rule=hit)
        event_bus.publish("blacklist_match", {"job_id": self.job.get("id"), "url": url, "depth": depth, "source": source, "rule": hit, "from_url": from_url})
        return True

    def priority(self, url, depth, source):
        u = url.lower(); score = depth * 10
        if source == "pattern": score -= 3
        if any(x in u for x in ["directory", "categories", "company", "exchanger", "exchangers", "market", "service", "online-banking", "payment-systems", "cryptocurrencies", "banks", "wallet"]): score -= 6
        if self.masked_detail_boost and any(x in u for x in ["/online-banking/", "/exchangers/", "/payment-systems/", "/cryptocurrencies/", "/trading-platforms/"]): score -= 8
        if "/api" in u: score += 4
        return score

    async def run(self):
        _opt = runtime_config.is_optimized()
        if _opt:
            conn_limit = int(runtime_config.get("aiohttp_total_conns") or 4)
            conn_per_host = int(runtime_config.get("aiohttp_per_host") or 2)
        else:
            conn_limit = int(self.job.get("concurrency", 12))
            conn_per_host = int(self.job.get("concurrency_per_domain", 4))
        # Hard cap to avoid pathological values coming from old configs.
        conn_limit = max(1, min(conn_limit, 64))
        conn_per_host = max(1, min(conn_per_host, conn_limit))
        conn = aiohttp.TCPConnector(
            limit_per_host=conn_per_host,
            limit=conn_limit,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        async with aiohttp.ClientSession(connector=conn) as session:
            browser = None
            pw = None
            wants_browser = self.use_playwright or ((self.masked_outbound_aggressive or self.auto_pagination) and async_playwright is not None)
            if wants_browser:
                # Hold a global Playwright slot so we don't spawn N chromiums in parallel.
                if not _acquire_playwright_slot(timeout=120.0):
                    event_bus.publish("playwright_slot_timeout", {"job_id": self.job.get("id")})
                else:
                    self._playwright_slot_held = True
                    try:
                        pw = await async_playwright().start()
                        launch_args = []
                        if _opt and runtime_config.get("playwright_lightweight_args"):
                            launch_args = [
                                "--no-sandbox",
                                "--disable-dev-shm-usage",
                                "--disable-gpu",
                                "--disable-extensions",
                                "--disable-background-networking",
                                "--disable-background-timer-throttling",
                                "--disable-renderer-backgrounding",
                                "--disable-features=TranslateUI,site-per-process",
                                "--mute-audio",
                                "--no-first-run",
                                "--no-default-browser-check",
                            ]
                        browser = await pw.chromium.launch(headless=True, args=launch_args)
                    except Exception as e:
                        event_bus.publish("playwright_launch_failed", {"job_id": self.job.get("id"), "error": str(e)})
                        browser = None
                        try:
                            if pw: await pw.stop()
                        except Exception:
                            pass
                        pw = None
                        _release_playwright_slot()
                        self._playwright_slot_held = False
            try:
                pq = []
                pages = 0
                cp = self._load_checkpoint()
                if cp and cp.get("queue"):
                    pages = int(cp.get("pages") or 0)
                    self.pages_done = pages
                    for row in cp.get("queue", []):
                        heapq.heappush(pq, (row.get("priority", 0), int(row.get("depth", 0)), row.get("url"), row.get("source", "crawl"), row.get("parent", "")))
                    event_bus.publish("job_resumed_from_checkpoint", {"job_id": self.job.get("id"), "pages": pages, "queued": len(pq), "saved_at": cp.get("saved_at")})
                else:
                    heapq.heappush(pq, (0, 0, self.root, "seed", ""))
                    self.dedupe.add(self.root)
                self._save_checkpoint(pq, pages, force=True)
                while pq and pages < self.max_pages:
                    await self._control_gate()
                    _, depth, url, source, parent = heapq.heappop(pq)
                    if not url:
                        continue
                    if depth > self.depth:
                        self.record_skip(url, "max_depth_exceeded", depth=depth, max_depth=self.depth, parent=parent)
                        self._save_checkpoint(pq, pages, url)
                        continue
                    if self.should_skip_blacklisted(url, depth, from_url=parent, source=source):
                        self._save_checkpoint(pq, pages, url)
                        continue
                    await self._control_gate(url)
                    res = await (self.fetch_playwright(browser, url) if browser and depth == 0 else self.fetch_http(session, url))
                    await self._control_gate(url)
                    final_url = canonicalize(res.get("url") or url)
                    html = res.get("html") or ""
                    pages += 1
                    self.pages_done = pages
                    self.urls_seen.append(final_url)
                    node = self.graph.add_node(final_url, depth=depth, type=classify_site(final_url, html), source=source, pattern_match=(source=="pattern"), status_code=res.get("status",0), confidence=0.91 if source=="pattern" else 0.65, parent=parent or None, content_type=res.get("content_type",""))
                    event_bus.publish("node_discovered", {"job_id": self.job.get("id"), **node})
                    if parent:
                        edge = self.graph.add_edge(parent, final_url, source=source if source != "seed" else "crawl")
                        event_bus.publish("edge_created", {"job_id": self.job.get("id"), **edge})
                    if res.get("status",0) >= 400 or not html:
                        self.record_skip(final_url, "fetch_empty_or_error", status=res.get("status",0), error=res.get("error"), depth=depth)
                        event_bus.publish("job_progress", {"job_id": self.job["id"], "pages": pages, "queued": len(pq), "url": final_url, "elapsed_seconds": int(time.time() - float(self.job.get("started_unix") or self.started_at))})
                        self._save_checkpoint(pq, pages, final_url, force=True)
                        continue
                    raw_links = [canonicalize(l) for l in extract_links(final_url, html)]
                    if self.auto_pagination:
                        # Static pagination links first, then browser-driven pagination for DataTables/client-side lists.
                        extra_links = set(self.extract_pagination_links_from_html(final_url, html))
                        if browser and self.looks_paginated_html(html):
                            extra_links.update(await self.extract_paginated_links_with_browser(browser, final_url))
                        if extra_links:
                            before = len(raw_links)
                            raw_links = list(dict.fromkeys(raw_links + list(extra_links)))
                            event_bus.publish("pagination_links_collected", {"job_id": self.job.get("id"), "url": final_url, "added": len(raw_links)-before, "total_links": len(raw_links)})
                    masked_candidates = set()
                    if self.follow_masked_outbound:
                        masked_candidates = await self.collect_masked_outbound(session, raw_links, final_url, depth, html=html, browser=browser)
                    links = []
                    for l in raw_links:
                        if l in masked_candidates:
                            continue
                        if not same_domain(self.root, l):
                            self.record_skip(l, "external_domain_not_crawled", from_url=final_url, depth=depth)
                            continue
                        links.append(l)
                    for l in links:
                        await self._control_gate(l)
                        l = canonicalize(l)
                        if self.should_skip_blacklisted(l, depth+1, from_url=final_url, source="crawl"):
                            continue
                        if self.dedupe.add(l):
                            heapq.heappush(pq, (self.priority(l, depth+1, "crawl"), depth+1, l, "crawl", final_url))
                        else:
                            self.record_skip(l, "duplicate_url", from_url=final_url, depth=depth)
                    if self.options.get("route_inference", True):
                        for route in COMMON_ROUTES:
                            candidate = f"{urlparse(self.root).scheme}://{self.domain}{route}"
                            if self.should_skip_blacklisted(candidate, depth+1, from_url=final_url, source="inferred"):
                                continue
                            if self.dedupe.add(candidate):
                                heapq.heappush(pq, (self.priority(candidate, depth+1, "inferred"), depth+1, candidate, "inferred", final_url))
                            else:
                                self.record_skip(candidate, "duplicate_inferred_route", from_url=final_url, depth=depth)
                    if self.options.get("pattern_expansion", True) and len(self.urls_seen) >= 2:
                        new_patterns = detect_patterns(self.urls_seen[-80:])
                        await self.apply_patterns(session, new_patterns, pq, depth, final_url)
                    event_bus.publish("job_progress", {"job_id": self.job["id"], "pages": pages, "queued": len(pq), "url": final_url, "elapsed_seconds": int(time.time() - float(self.job.get("started_unix") or self.started_at)), "external_resolved": len(self.external_report.get("resolved", [])), "external_failed": len(self.external_report.get("failed", []))})
                    self._save_checkpoint(pq, pages, final_url)
                if pq and pages >= self.max_pages:
                    for _, d, u, src, par in list(pq)[:500]:
                        self.record_skip(u, "max_pages_reached", depth=d, source=src, parent=par, max_pages=self.max_pages)
                self._save_checkpoint(pq, pages, force=True)
                return {"graph": self.graph.as_dict(), "patterns": self.patterns, "pages": pages, "urls": self.urls_seen, "skipped": self.skipped, "external_report": self.external_report, "pagination_report": self.pagination_report, "checkpoint": checkpoint_store.get(self.job.get("id"))}
            finally:
                if browser:
                    try: await browser.close()
                    except Exception: pass
                if pw:
                    try: await pw.stop()
                    except Exception: pass
                if self._playwright_slot_held:
                    _release_playwright_slot()
                    self._playwright_slot_held = False

    async def apply_patterns(self, session, patterns, pq, depth, parent):
        await self._control_gate(parent)
        known = {(p.get('domain'), p.get('pattern')) for p in self.patterns}
        for p in patterns:
            if (p.get('domain'), p.get('pattern')) not in known:
                self.patterns.append(p); known.add((p.get('domain'), p.get('pattern')))
                event_bus.publish("pattern_detected", {"job_id": self.job.get("id"), **p})
            if not self.options.get("hypothetical_urls", True): continue
            values = list(dict.fromkeys((p.get("active_values") or []) + (p.get("suggested_values") or [])[:8]))
            for v in values:
                await self._control_gate(parent)
                u = build_url(p["domain"], p["pattern"], v)
                if not same_domain(self.root, u): continue
                if self.should_skip_blacklisted(u, depth+1, from_url=parent, source="pattern"):
                    continue
                if not self.dedupe.add(u): continue
                if self.options.get("soft_probe", True):
                    probe = await soft_probe(session, u)
                    if not probe.get("valid"):
                        self.record_skip(u, "soft_probe_invalid", pattern=p.get("pattern"), value=v, parent=parent)
                        continue
                    u = canonicalize(probe.get("url", u))
                heapq.heappush(pq, (self.priority(u, depth+1, "pattern"), depth+1, u, "pattern", parent))
                event_bus.publish("pattern_applied", {"job_id": self.job.get("id"), "pattern": p["pattern"], "value": v, "url": u})
        for seed in active_seeds(self.domain):
            for v in seed.get("values", [])[:20]:
                u = build_url(seed["domain"], seed["pattern"], v)
                if self.should_skip_blacklisted(u, depth+1, from_url=parent, source="seed"):
                    continue
                if self.dedupe.add(u):
                    heapq.heappush(pq, (self.priority(u, depth+1, "seed"), depth+1, u, "seed", parent))
                else:
                    self.record_skip(u, "duplicate_seed_url", pattern=seed.get("pattern"), value=v, parent=parent)
