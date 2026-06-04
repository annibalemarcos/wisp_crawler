from __future__ import annotations
import re, time
from urllib.parse import urlparse
from storage.json_store import JsonStore
from utils.hashing import stable_id

blacklist_store = JsonStore("blacklist_dirs")

DEFAULT_RULES = [
    ("login", "contains", "Common login/auth page"),
    ("signin", "contains", "Common login/auth page"),
    ("sign-in", "contains", "Common login/auth page"),
    ("signup", "contains", "Common signup page"),
    ("sign-up", "contains", "Common signup page"),
    ("register", "contains", "Common registration page"),
    ("account", "contains", "Account area"),
    ("dashboard", "contains", "Private/account area"),
    ("checkout", "contains", "Checkout flow"),
    ("cart", "contains", "Cart flow"),
    ("privacy", "contains", "Legal noise"),
    ("terms", "contains", "Legal noise"),
    ("about", "contains", "About/company info noise"),
    ("contact", "contains", "Contact page noise"),
    (r"/(?:wp-admin|wp-login)(?:/|$|\?)", "regex", "WordPress admin/login noise"),
]

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _normalize_simple(pattern: str) -> str:
    p = (pattern or "").strip().lower()
    while p.startswith("/"):
        p = p[1:]
    while p.endswith("/") and len(p) > 1:
        p = p[:-1]
    return p

def ensure_defaults() -> None:
    if blacklist_store.list():
        return
    for pattern, kind, note in DEFAULT_RULES:
        add_rule({"pattern": pattern, "kind": kind, "enabled": True, "note": note, "scope": "global", "built_in": True})

def list_rules() -> list[dict]:
    ensure_defaults()
    return blacklist_store.list()

def add_rule(data: dict) -> dict:
    pattern = (data.get("pattern") or "").strip()
    if not pattern:
        raise ValueError("pattern is required")
    kind = (data.get("kind") or data.get("type") or "contains").strip().lower()
    if kind not in ("contains", "regex"):
        kind = "contains"
    rid = data.get("id") or stable_id(kind, pattern.lower())
    item = {
        "id": rid,
        "pattern": pattern,
        "kind": kind,
        "enabled": bool(data.get("enabled", True)),
        "note": data.get("note", ""),
        "scope": data.get("scope", "global"),
        "built_in": bool(data.get("built_in", False)),
        "created_at": data.get("created_at") or _now_iso(),
        "updated_at": _now_iso(),
    }
    return blacklist_store.put(rid, item)

def update_rule(rid: str, data: dict) -> dict | None:
    item = blacklist_store.get(rid)
    if not item:
        return None
    item.update({k: v for k, v in data.items() if k in {"pattern", "kind", "enabled", "note", "scope"}})
    item["kind"] = (item.get("kind") or "contains").lower()
    if item["kind"] not in ("contains", "regex"):
        item["kind"] = "contains"
    item["updated_at"] = _now_iso()
    return blacklist_store.put(rid, item)

def delete_rule(rid: str) -> bool:
    return blacklist_store.delete(rid)

def replace_rules(rows: list[dict]) -> list[dict]:
    for row in list_rules():
        blacklist_store.delete(row["id"])
    out = []
    for row in rows:
        if (row.get("pattern") or "").strip():
            out.append(add_rule(row))
    return out

def explain_match(url: str, rules: list[dict] | None = None) -> dict | None:
    """Return the first enabled blacklist rule that matches a URL.

    Simple contains rules are slash-optional: "login", "/login" and "login/" all
    match /login and /partners-login. Regex rules are applied to full URL, path,
    and path+query with case-insensitive mode.
    """
    rules = rules if rules is not None else list_rules()
    parsed = urlparse(url)
    path = parsed.path or "/"
    path_query = path + (("?" + parsed.query) if parsed.query else "")
    clean_path = path.lower().strip("/")
    full = url.lower()
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        pat = (rule.get("pattern") or "").strip()
        if not pat:
            continue
        kind = (rule.get("kind") or "contains").lower()
        if kind == "regex":
            try:
                if re.search(pat, url, flags=re.I) or re.search(pat, path_query, flags=re.I) or re.search(pat, path, flags=re.I):
                    return {"rule_id": rule.get("id"), "pattern": pat, "kind": kind, "note": rule.get("note", "")}
            except re.error as e:
                continue
        else:
            simple = _normalize_simple(pat)
            if simple and (simple in clean_path or simple in full):
                return {"rule_id": rule.get("id"), "pattern": pat, "kind": kind, "note": rule.get("note", "")}
    return None
