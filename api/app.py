from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import json, os, re, time
from pathlib import Path
from flask import Flask, jsonify, request, Response, render_template
from flask_cors import CORS
from utils.hashing import stable_id
from storage.job_store import job_store
from storage.pattern_store import pattern_store, seed_store
from storage.service_store import service_store
from graph.graph_store import graph_store
from core.scheduler import start_job, list_jobs, get_job, pause_job, resume_job, restart_job_from_checkpoint, cancel_job, delete_job, notify_optimization_changed, apply_windows_priority_once
from storage.blacklist_store import list_rules as list_blacklist_rules, add_rule as add_blacklist_rule, update_rule as update_blacklist_rule, delete_rule as delete_blacklist_rule, replace_rules as replace_blacklist_rules
from patterns.suggest import suggest_from_payload
from patterns.seeds import create_seed
from storage.event_bus import event_bus
from core import runtime_config

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
app = Flask(__name__, template_folder=str(ROOT / "dashboard" / "templates"), static_folder=str(ROOT / "dashboard" / "static"))
CORS(app)

DATA_COLLECTIONS = ("jobs", "graphs", "checkpoints", "patterns", "pattern_seeds", "services", "blacklist_dirs")
GLOBAL_CLEAR_CONFIRM = "DELETE WISP DATA"
GLOBAL_RESTORE_CONFIRM = "RESTORE WISP BACKUP"
ACTIVE_JOB_STATUSES = ("queued", "running", "paused", "pause_requested", "restarting", "cancel_requested")

def _is_live_active_job(job: dict) -> bool:
    return job.get("status") in ACTIVE_JOB_STATUSES and bool(job.get("is_thread_alive"))

def _is_stale_active_job(job: dict) -> bool:
    return job.get("status") in ACTIVE_JOB_STATUSES and not bool(job.get("is_thread_alive"))

def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _data_status() -> dict:
    collections = {}
    total_files = 0
    total_bytes = 0
    for name in DATA_COLLECTIONS:
        d = DATA_DIR / name
        files = list(d.glob("*.json")) if d.exists() else []
        size = sum((p.stat().st_size for p in files if p.exists()), 0)
        collections[name] = {"count": len(files), "bytes": size}
        total_files += len(files); total_bytes += size
    events_file = DATA_DIR / "events.jsonl"
    events_bytes = events_file.stat().st_size if events_file.exists() else 0
    events_count = 0
    if events_file.exists():
        try:
            events_count = sum(1 for _ in events_file.open("r", encoding="utf-8", errors="ignore"))
        except Exception:
            events_count = 0
    total_bytes += events_bytes
    jobs = list_jobs()
    active = [j for j in jobs if _is_live_active_job(j)]
    stale_active = [j for j in jobs if _is_stale_active_job(j)]
    return {"collections": collections, "events": {"count": events_count, "bytes": events_bytes}, "total_files": total_files, "total_bytes": total_bytes, "active_jobs": len(active), "active_job_ids": [j.get("id") for j in active if j.get("id")], "stale_active_jobs": len(stale_active), "stale_active_job_ids": [j.get("id") for j in stale_active if j.get("id")], "generated_at": _now_iso()}

def _read_collection(name: str) -> dict:
    out = {}
    d = DATA_DIR / name
    if not d.exists():
        return out
    for p in sorted(d.glob("*.json")):
        try:
            out[p.stem] = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            out[p.stem] = {"id": p.stem, "_backup_error": str(e), "_raw": p.read_text(encoding="utf-8", errors="ignore")}
    return out

def _events_backup() -> list[str]:
    p = DATA_DIR / "events.jsonl"
    if not p.exists():
        return []
    return p.read_text(encoding="utf-8", errors="ignore").splitlines()

def _build_backup() -> dict:
    return {"format": "wisp-global-backup", "version": 1, "created_at": _now_iso(), "status": _data_status(), "collections": {name: _read_collection(name) for name in DATA_COLLECTIONS}, "events_jsonl": _events_backup()}

def _active_jobs_blocking() -> list[dict]:
    # Persisted "running" jobs can outlive their worker thread after a restart or crash.
    # Those zombie rows should not prevent a deliberate global wipe/restore.
    return [j for j in list_jobs() if _is_live_active_job(j)]

def _job_blockers_response(active: list[dict]) -> dict:
    return {
        "count": len(active),
        "jobs": [
            {
                "id": j.get("id"),
                "status": j.get("status"),
                "url": j.get("url"),
                "seconds_since_heartbeat": j.get("seconds_since_heartbeat"),
                "is_thread_alive": j.get("is_thread_alive"),
            }
            for j in active[:25]
        ],
    }

def _clear_data_files(include_events: bool = True) -> dict:
    deleted = {"collections": {}, "events_deleted": False}
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for name in DATA_COLLECTIONS:
        d = DATA_DIR / name
        d.mkdir(parents=True, exist_ok=True)
        count = 0
        for p in list(d.glob("*.json")):
            p.unlink()
            count += 1
        deleted["collections"][name] = count
    if include_events:
        events_file = DATA_DIR / "events.jsonl"
        if events_file.exists():
            events_file.unlink()
            deleted["events_deleted"] = True
        try:
            event_bus.clear_memory_only()
        except Exception:
            pass
    return deleted

def _write_collection(name: str, rows) -> int:
    d = DATA_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, list):
        iterable = [(str((item or {}).get("id") or stable_id(name, str(i))), item) for i, item in enumerate(rows)]
    elif isinstance(rows, dict):
        iterable = rows.items()
    else:
        iterable = []
    count = 0
    for item_id, item in iterable:
        if not isinstance(item, dict):
            continue
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(item_id or item.get("id") or stable_id(name, str(count))))
        item.setdefault("id", safe_id)
        (d / f"{safe_id}.json").write_text(json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")
        count += 1
    return count

def _restore_backup(backup: dict, replace: bool = True, include_events: bool = True) -> dict:
    if not isinstance(backup, dict) or backup.get("format") != "wisp-global-backup":
        raise ValueError("invalid WISP backup")
    if backup.get("version") != 1 or not isinstance(backup.get("collections"), dict):
        raise ValueError("invalid WISP backup structure")
    if replace:
        _clear_data_files(include_events=include_events)
    collections = backup.get("collections") or {}
    restored = {"collections": {}, "events": 0}
    for name in DATA_COLLECTIONS:
        restored["collections"][name] = _write_collection(name, collections.get(name) or {})
    if include_events:
        events = backup.get("events_jsonl") or []
        if isinstance(events, list):
            events_file = DATA_DIR / "events.jsonl"
            lines = [str(x) for x in events if str(x).strip()]
            text = "\n".join(lines) + ("\n" if lines else "")
            if replace:
                events_file.write_text(text, encoding="utf-8")
            elif text:
                with events_file.open("a", encoding="utf-8") as f:
                    f.write(text)
            restored["events"] = len(events)
            try:
                event_bus.clear_memory_only()
                event_bus._load_from_disk()
            except Exception:
                pass
    return restored


def _event_job_id(row: dict) -> str:
    payload = row.get("payload") or {}
    return payload.get("job_id") or payload.get("id") or payload.get("jobId") or ""

def _synthetic_events_from_state(limit: int = 1000) -> list[dict]:
    """Build useful log rows from persisted jobs/graphs when the live event buffer is empty.
    This avoids a blank Live Events page after server restarts or older jobs created before
    persistent event logging existed.
    """
    rows: list[dict] = []
    now = time.time()
    try:
        jobs = job_store.list()
    except Exception:
        jobs = []
    for j in reversed(jobs[-200:]):
        jid = j.get("id")
        created = j.get("created_at") or j.get("updated_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
        rows.append({"event":"job_state", "payload":{"job_id":jid, "url":j.get("url"), "status":j.get("status"), "progress":j.get("progress",{}), "synthetic":True}, "ts":created, "ts_unix":now})
        try:
            g = graph_store.get(jid) or {}
        except Exception:
            g = {}
        ext = g.get("external_report") or {}
        for item in (ext.get("resolved") or [])[:200]:
            rows.append({"event":"masked_outbound_resolved", "payload":{"job_id":jid, **item, "synthetic":True}, "ts":created, "ts_unix":now})
        for item in (ext.get("failed") or [])[:200]:
            rows.append({"event":"masked_outbound_failed", "payload":{"job_id":jid, **item, "synthetic":True}, "ts":created, "ts_unix":now})
        skipped = g.get("skipped") or []
        for item in skipped[:200]:
            rows.append({"event":"url_skipped", "payload":{"job_id":jid, **item, "synthetic":True}, "ts":created, "ts_unix":now})
    return rows[-limit:]

@app.get("/")
def dashboard():
    return render_template("index.html")

@app.get("/api/jobs")
def api_jobs(): return jsonify(list_jobs())

@app.get("/api/jobs/<jid>")
def api_job(jid):
    j = get_job(jid)
    if not j: return jsonify({"error":"job not found"}), 404
    return jsonify(j)

@app.post("/api/run")
def api_run():
    data = request.get_json(force=True)
    if not data.get("url"): return jsonify({"error":"url is required"}), 400
    return jsonify(start_job(data)), 202

@app.post("/api/bulk-run")
def api_bulk_run():
    data = request.get_json(force=True)
    urls = data.get("urls") or []
    jobs = [start_job({**data, "url": u}) for u in urls if u]
    return jsonify({"jobs": jobs, "count": len(jobs)}), 202

@app.post("/api/massive-run")
def api_massive_run():
    """Create many connected jobs from root/seed entries.

    Payload accepts either:
      entries: [{root_url, seed_url, label, enabled}]
      urls: ["https://..."]  # fallback
    For entries with both root_url and seed_url, both may be launched depending
    on launch_roots / launch_seeds. Each job keeps massive_batch_id metadata.
    """
    data = request.get_json(force=True)
    entries = data.get("entries") or []
    if not entries and data.get("urls"):
        entries = [{"seed_url": u, "root_url": "", "label": ""} for u in data.get("urls") or []]
    batch_id = stable_id("massive", str(time.time()))
    launch_roots = data.get("launch_roots", True)
    launch_seeds = data.get("launch_seeds", True)
    jobs = []
    seen = set()
    for idx, row in enumerate(entries):
        if not row or row.get("enabled") is False:
            continue
        label = row.get("label") or row.get("name") or f"node-{idx+1}"
        targets = []
        if launch_roots and row.get("root_url"):
            targets.append(("root", row.get("root_url")))
        if launch_seeds and row.get("seed_url"):
            targets.append(("seed", row.get("seed_url")))
        if not targets and row.get("url"):
            targets.append(("seed", row.get("url")))
        for node_type, url in targets:
            if not url or url in seen:
                continue
            seen.add(url)
            payload = {**data, "url": url}
            payload.pop("entries", None); payload.pop("urls", None)
            payload["mode"] = "massive_job"
            payload["massive_batch_id"] = batch_id
            payload["massive_label"] = label
            payload["massive_node_type"] = node_type
            j = start_job(payload)
            # store metadata after creation
            full = job_store.get(j["id"]) or j
            full.update({"massive_batch_id": batch_id, "massive_label": label, "massive_node_type": node_type, "massive_index": idx})
            job_store.put(full["id"], full)
            jobs.append(full)
    event_bus.publish("massive_job_created", {"batch_id": batch_id, "jobs": len(jobs), "entries": len(entries)})
    return jsonify({"batch_id": batch_id, "jobs": jobs, "count": len(jobs), "entries": len(entries)}), 202

@app.get("/api/blacklist-dirs")
def api_blacklist_dirs():
    return jsonify({"rules": list_blacklist_rules()})

@app.post("/api/blacklist-dirs")
def api_blacklist_add():
    try:
        return jsonify(add_blacklist_rule(request.get_json(force=True))), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.put("/api/blacklist-dirs")
def api_blacklist_replace():
    data = request.get_json(force=True)
    rows = data.get("rules") if isinstance(data, dict) else data
    return jsonify({"rules": replace_blacklist_rules(rows or [])})

@app.put("/api/blacklist-dirs/<rid>")
def api_blacklist_update(rid):
    item = update_blacklist_rule(rid, request.get_json(force=True))
    if not item:
        return jsonify({"error": "rule not found"}), 404
    return jsonify(item)

@app.delete("/api/blacklist-dirs/<rid>")
def api_blacklist_delete(rid):
    return jsonify({"deleted": delete_blacklist_rule(rid), "id": rid})

@app.get("/api/results")
def api_results():
    jid = request.args.get("job_id")
    if jid: return jsonify({"job": job_store.get(jid), "graph": graph_store.get(jid)})
    return jsonify([{"job": j, "graph": graph_store.get(j["id"])} for j in job_store.list()])

@app.get("/api/stats")
def api_stats():
    jobs = job_store.list(); patterns = pattern_store.list(); services = service_store.list(); graphs = graph_store.list()
    nodes = sum(len(g.get("nodes",[])) for g in graphs); edges = sum(len(g.get("edges",[])) for g in graphs)
    return jsonify({"jobs": len(jobs), "running": len([j for j in jobs if j.get('status')=='running']), "patterns": len(patterns), "services": len(services), "nodes": nodes, "edges": edges})

@app.get("/api/global-config/status")
def api_global_config_status():
    return jsonify(_data_status())

@app.get("/api/global-config/backup")
def api_global_config_backup():
    backup = _build_backup()
    payload = json.dumps(backup, ensure_ascii=False, separators=(",", ":"))
    filename = f"wisp-global-backup-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}.json"
    return Response(
        payload,
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.post("/api/global-config/import")
def api_global_config_import():
    data = request.get_json(force=True, silent=True) or {}
    if data.get("confirm") != GLOBAL_RESTORE_CONFIRM:
        return jsonify({"error": f"type {GLOBAL_RESTORE_CONFIRM!r} to restore a global backup"}), 400
    active = _active_jobs_blocking()
    if active:
        return jsonify({"error": "active jobs must be stopped before restore", "active_jobs": _job_blockers_response(active)}), 409
    backup = data.get("backup") or data
    try:
        restored = _restore_backup(
            backup,
            replace=data.get("replace", True) is not False,
            include_events=data.get("include_events", True) is not False,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"restored": restored, "status": _data_status()})

@app.post("/api/global-config/clear")
def api_global_config_clear():
    data = request.get_json(force=True, silent=True) or {}
    if data.get("confirm") != GLOBAL_CLEAR_CONFIRM:
        return jsonify({"error": f"type {GLOBAL_CLEAR_CONFIRM!r} to clear all data"}), 400
    active = _active_jobs_blocking()
    if active:
        return jsonify({"error": "active jobs must be stopped before clearing data", "active_jobs": _job_blockers_response(active)}), 409
    deleted = _clear_data_files(include_events=data.get("include_events", True) is not False)
    return jsonify({"deleted": deleted, "status": _data_status()})

@app.get("/api/progress")
def api_progress():
    return jsonify([{ "id": j["id"], "status": j.get("status"), "progress": j.get("progress", {}), "elapsed_seconds": j.get("elapsed_seconds",0), "checkpoint": j.get("checkpoint"), "last_heartbeat": j.get("last_heartbeat"), "seconds_since_heartbeat": j.get("seconds_since_heartbeat") } for j in list_jobs()])

@app.get("/api/stream")
def api_stream():
    replay = request.args.get("replay", "80")
    try:
        replay = max(0, min(int(replay), 5000))
    except Exception:
        replay = 80
    return Response(
        event_bus.subscribe(replay=replay),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

@app.get("/api/events")
def api_events():
    limit = request.args.get("limit", "1000")
    job_id = request.args.get("job_id")
    event_type = request.args.get("event")
    include_synthetic = request.args.get("synthetic", "1") not in ("0", "false", "False", "no")
    try:
        limit = max(1, min(int(limit), 10000))
    except Exception:
        limit = 1000
    rows = event_bus.snapshot(limit=limit)

    # If there is no in-memory/file event history yet, reconstruct a useful console
    # from persisted jobs and graph reports. The rows are marked synthetic=True.
    if include_synthetic and not rows:
        rows = _synthetic_events_from_state(limit=limit)

    if job_id:
        rows = [r for r in rows if _event_job_id(r) == job_id]
    if event_type:
        rows = [r for r in rows if r.get("event") == event_type]
    return jsonify({"events": rows[-limit:], "count": len(rows[-limit:]), "persistent": True})

@app.get("/api/graph")
def api_graph():
    jid = request.args.get("job_id")
    if jid: return jsonify(graph_store.get(jid) or {"nodes":[],"edges":[]})
    merged = {"nodes": [], "edges": []}
    seen_n, seen_e = set(), set()
    for g in graph_store.list():
        for n in g.get("nodes", []):
            if n.get("id") not in seen_n: seen_n.add(n.get("id")); merged["nodes"].append(n)
        for e in g.get("edges", []):
            if e.get("id") not in seen_e: seen_e.add(e.get("id")); merged["edges"].append(e)
    return jsonify(merged)

@app.get("/api/patterns")
def get_patterns(): return jsonify(pattern_store.list())

@app.post("/api/patterns")
def post_pattern():
    d = request.get_json(force=True); pid = d.get("id") or stable_id(d.get("domain",""), d.get("pattern",""), str(time.time()))
    item = {"id": pid, "domain": d.get("domain",""), "pattern": d.get("pattern",""), "type": d.get("type","manual"), "confidence": d.get("confidence", 1), "examples": d.get("examples", []), "suggested_values": d.get("suggested_values", []), "active_values": d.get("active_values", d.get("values", [])), "enabled": d.get("enabled", True), "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    return jsonify(pattern_store.put(pid, item)), 201

@app.put("/api/patterns/<pid>")
def put_pattern(pid):
    item = pattern_store.get(pid) or {"id": pid}; item.update(request.get_json(force=True))
    return jsonify(pattern_store.put(pid, item))

@app.delete("/api/patterns/<pid>")
def del_pattern(pid):
    ok = pattern_store.delete(pid); event_bus.publish("pattern_deleted", {"id": pid})
    return jsonify({"deleted": ok, "id": pid})

@app.get("/api/pattern-seeds")
def get_seeds(): return jsonify(seed_store.list())

@app.post("/api/pattern-seeds")
def post_seed(): return jsonify(create_seed(request.get_json(force=True))), 201

@app.put("/api/pattern-seeds/<sid>")
def put_seed(sid):
    item = seed_store.get(sid) or {"id": sid}; item.update(request.get_json(force=True)); item["version"] = int(item.get("version",1))+1
    return jsonify(seed_store.put(sid, item))

@app.delete("/api/pattern-seeds/<sid>")
def del_seed(sid): return jsonify({"deleted": seed_store.delete(sid), "id": sid})

@app.post("/api/pattern-suggest")
def pattern_suggest(): return jsonify(suggest_from_payload(request.get_json(force=True)))

@app.post("/api/patterns/<pid>/values")
def add_pattern_value(pid):
    d = request.get_json(force=True); value = d.get("value")
    item = pattern_store.get(pid)
    if not item: return jsonify({"error":"pattern not found"}), 404
    vals = item.setdefault("active_values", [])
    if value and value not in vals: vals.append(value)
    pattern_store.put(pid, item); event_bus.publish("pattern_value_added", {"id": pid, "value": value})
    return jsonify(item)

@app.delete("/api/patterns/<pid>/values/<path:value>")
def remove_pattern_value(pid, value):
    item = pattern_store.get(pid)
    if not item: return jsonify({"error":"pattern not found"}), 404
    item["active_values"] = [v for v in item.get("active_values", []) if v != value]
    pattern_store.put(pid, item); event_bus.publish("pattern_value_removed", {"id": pid, "value": value})
    return jsonify(item)

@app.post("/api/jobs/<jid>/rerun")
def rerun_job(jid):
    old = job_store.get(jid)
    if not old: return jsonify({"error":"job not found"}), 404
    payload = {**old, **request.get_json(force=True), "url": old["url"]}
    return jsonify(start_job(payload, rerun_of=jid)), 202


@app.post("/api/jobs/<jid>/pause")
def api_pause_job(jid):
    j = pause_job(jid)
    if not j: return jsonify({"error":"job not found"}), 404
    return jsonify(j)

@app.post("/api/jobs/<jid>/resume")
def api_resume_job(jid):
    j = resume_job(jid)
    if not j: return jsonify({"error":"job not found"}), 404
    return jsonify(j)

@app.post("/api/jobs/<jid>/restart")
def api_restart_job(jid):
    j = restart_job_from_checkpoint(jid)
    if not j: return jsonify({"error":"job not found"}), 404
    return jsonify(j), 202

@app.post("/api/jobs/<jid>/cancel")
def api_cancel_job(jid):
    j = cancel_job(jid)
    if not j: return jsonify({"error":"job not found"}), 404
    return jsonify(j)

@app.delete("/api/jobs/<jid>")
def api_delete_job(jid):
    return jsonify(delete_job(jid))

@app.get("/api/services")
def get_services(): return jsonify(service_store.list())

@app.delete("/api/services/<sid>")
def delete_service(sid):
    svc = service_store.get(sid)
    ok = service_store.delete(sid)
    # Hard-delete associated graph fragments by removing nodes from matching domain.
    if svc:
        domain = svc.get("domain")
        for g in graph_store.list():
            nodes = [n for n in g.get("nodes", []) if domain not in n.get("url","")]
            kept = {n.get("id") for n in nodes}
            edges = [e for e in g.get("edges", []) if e.get("from") in kept and e.get("to") in kept]
            g["nodes"], g["edges"] = nodes, edges
            graph_store.put(g["id"], g)
    event_bus.publish("service_deleted", {"id": sid, "domain": svc.get("domain") if svc else None})
    return jsonify({"deleted": ok, "id": sid})


@app.get('/api/external-report')
def api_external_report():
    jid = request.args.get('job_id')
    graphs = [graph_store.get(jid)] if jid else graph_store.list()
    report = {'candidates': [], 'resolved': [], 'failed': [], 'errors': []}
    for g in graphs:
        if not g: continue
        jr = g.get('external_report') or {}
        for k in report:
            for item in jr.get(k, []) or []:
                report[k].append({'job_id': g.get('job_id') or g.get('id'), **item})
    return jsonify(report)

@app.get('/api/skipped')
def api_skipped():
    jid = request.args.get('job_id')
    graphs = [graph_store.get(jid)] if jid else graph_store.list()
    rows = []
    for g in graphs:
        if not g: continue
        for item in g.get('skipped', []) or []:
            rows.append({'job_id': g.get('job_id') or g.get('id'), **item})
    return jsonify({'skipped': rows, 'count': len(rows)})

@app.post('/api/pattern-lab/preview')
def api_pattern_lab_preview():
    from patterns.inference import build_url
    d = request.get_json(force=True)
    domain = d.get('domain','').strip()
    pattern = d.get('pattern','').strip()
    values = d.get('values') or []
    if isinstance(values, str):
        values = [v.strip() for v in values.replace('\n', ',').split(',') if v.strip()]
    urls = [build_url(domain, pattern, v) for v in values[:500] if domain and pattern]
    return jsonify({'urls': urls, 'count': len(urls)})

@app.post('/api/pattern-lab/probe')
def api_pattern_lab_probe():
    import asyncio, aiohttp
    from patterns.validators import soft_probe
    d = request.get_json(force=True)
    urls = d.get('urls') or []
    if isinstance(urls, str):
        urls = [u.strip() for u in urls.replace('\n', ',').split(',') if u.strip()]
    async def run():
        async with aiohttp.ClientSession() as session:
            out=[]
            for u in urls[:200]:
                try:
                    p = await soft_probe(session, u)
                    out.append({'url': u, **p})
                except Exception as e:
                    out.append({'url': u, 'valid': False, 'error': str(e)})
            return out
    return jsonify({'results': asyncio.run(run())})



# Config Import/Export + Directory Discovery Mode

def _truthy(v, default=False):
    if v is None: return default
    if isinstance(v, bool): return v
    return str(v).strip().lower() in {"1","true","yes","y","on","sim","ligado","enabled"}

def _first_int(text, default):
    import re
    if text is None: return default
    m = re.search(r"\d+", str(text))
    return int(m.group(0)) if m else default

def _parse_url_lines(block: str) -> list[str]:
    import re
    out=[]
    for line in (block or '').splitlines():
        line=line.strip().strip('-•* ')
        if not line or line.startswith('#'): continue
        found=re.findall(r"https?://[^\s,;]+", line)
        out.extend([u.rstrip(').,;') for u in found])
    return out

def _strip_markdown_fences(text: str) -> str:
    lines=[]
    for ln in (text or '').replace('\r\n','\n').replace('\r','\n').split('\n'):
        if ln.strip().startswith('```'):
            continue
        lines.append(ln)
    return '\n'.join(lines)

def _split_config_blocks(text: str) -> list[str]:
    import json, re
    raw = _strip_markdown_fences(text or '').strip()
    if not raw:
        return []
    try:
        d = json.loads(raw)
        if isinstance(d, list):
            return [json.dumps(x, ensure_ascii=False) for x in d if isinstance(x, dict)]
        if isinstance(d, dict) and isinstance(d.get('configs'), list):
            return [json.dumps(x, ensure_ascii=False) for x in d.get('configs') if isinstance(x, dict)]
        if isinstance(d, dict):
            return [raw]
    except Exception:
        pass
    markers = list(re.finditer(r'(?mi)^\s*#\s*.+\.(?:txt|md|json)\s*$', raw))
    if len(markers) > 1:
        out=[]
        for i, m in enumerate(markers):
            end = markers[i+1].start() if i+1 < len(markers) else len(raw)
            chunk = raw[m.start():end].strip()
            if chunk and re.search(r'(?mi)^={4,}\s*\n\s*TYPE\s*:?', chunk):
                out.append(chunk)
        if out:
            return out
    starts=[m.start() for m in re.finditer(r'(?mi)^={4,}\s*\n\s*TYPE\s*:?', raw)]
    if len(starts) > 1:
        return [raw[starts[i]:(starts[i+1] if i+1 < len(starts) else len(raw))].strip() for i in range(len(starts))]
    return [raw]

def _first_line(text: str, default: str = '') -> str:
    for ln in (text or '').splitlines():
        s=ln.strip()
        if s and not s.startswith('#'):
            return s
    return default

def _first_url(text: str, default: str = '') -> str:
    urls=_parse_url_lines(text or '')
    return urls[0] if urls else default

def _split_csvish(text: str) -> list[str]:
    import re
    out=[]
    for x in re.split(r'[\n,;]+', text or ''):
        x=x.strip()
        if x and x not in out:
            out.append(x)
    return out

def _parse_config_text(text: str) -> dict:
    import json, re
    raw = _strip_markdown_fences(text or '').replace('\r\n','\n').replace('\r','\n')
    warnings=[]
    try:
        if raw.strip().startswith('{'):
            d=json.loads(raw)
            if isinstance(d, dict):
                d.setdefault('warnings', [])
                d.setdefault('name', d.get('config_name') or d.get('source_label') or '')
                return d
    except Exception:
        pass
    lines=[ln.rstrip() for ln in raw.split('\n')]
    sections={}; current='MAIN'; buf=[]
    def flush(): sections[current]=('\n'.join(buf)).strip()
    i=0
    while i < len(lines):
        ln=lines[i].strip()
        if set(ln) <= {'='} and len(ln)>=4:
            flush(); buf=[]; j=i+1
            while j < len(lines) and not lines[j].strip(): j+=1
            if j < len(lines): current=lines[j].strip().strip(':').upper(); i=j
            else: current=f'SECTION_{i}'
        else:
            buf.append(lines[i])
        i+=1
    flush()
    body='\n'.join(sections.values())+'\n'+raw
    KNOWN_KEYS={'depth','max pages','pattern expansion','route inference','soft probe','use seeds','auto pagination','pagination limit','follow masked outbound','aggressive external resolver','external limit','external resolver timeout','filter external by theme','external theme filter','theme keywords','external theme keywords','use blacklist dirs','use blacklist','directory discovery mode','known directory url','root url','section root url','directory keywords','max sibling routes','find pagination','find listing cards','find outbound company links','find profile/detail pages','name'}
    # Detect lines that look like "Key: value" in the raw text and flag unknown options.
    # We deliberately skip URL/section-only content. The parser already accepts what it
    # knows via the regex helpers; this is purely operator-friendly feedback.
    SKIP_PREFIXES=('type','url','root','seed','seed_','seed-','re:','regex:','blacklist','config','http://','https://','#','-','*','•')
    for ln in raw.split('\n'):
        s=ln.strip()
        if not s or set(s)<={'='} or ':' not in s: continue
        low=s.lower()
        if any(low.startswith(p) for p in SKIP_PREFIXES): continue
        key=s.split(':',1)[0].strip().lower()
        # Some humans write things like "Max Pages : 100", "Max-Pages: 100", "Max_Pages: 100"
        norm=re.sub(r'[_\-]+',' ',key).strip()
        if not key or norm in KNOWN_KEYS or key in KNOWN_KEYS: continue
        warnings.append({'line':ln.strip(),'reason':f'unknown option "{key}"'})
    def find_bool(name, default=False):
        m=re.search(rf"{re.escape(name)}\s*:\s*([^\n]+)", body, re.I)
        return _truthy(m.group(1), default) if m else default
    def find_int(name, default):
        m=re.search(rf"{re.escape(name)}\s*:\s*([^\n]+)", body, re.I)
        if not m: return default
        # For ranges like "100-200" or "20–50" we keep the smaller value to stay safe.
        nums=re.findall(r"\d+", m.group(1))
        if len(nums)>=2:
            warnings.append({'line':m.group(0).strip(),'reason':f'range detected; using {nums[0]}'})
            return int(nums[0])
        return _first_int(m.group(1), default)
    def find_text(name, default=''):
        m=re.search(rf"{re.escape(name)}\s*:\s*([^\n]+)", body, re.I)
        return (m.group(1).strip() if m else default)
    typ=(sections.get('TYPE') or '').strip().splitlines()[0:1]
    typ=typ[0].strip() if typ else 'Single Job'
    name=_first_line(sections.get('NAME',''), '')
    if not name:
        mm=re.search(r'(?mi)^\s*#\s*(.+?)\s*$', raw)
        name=(mm.group(1).strip() if mm else '')
    urls=_parse_url_lines(sections.get('URL',''))
    root_seed_block=sections.get('URL ROOT-SEED','') or sections.get('ROOT-SEED','') or sections.get('ROOT-SEED URL','')
    root_seed_urls=_parse_url_lines(root_seed_block)
    roots=[]; seeds=[]
    for line in root_seed_block.splitlines():
        if re.search(r'\broot\b', line, re.I): roots += _parse_url_lines(line)
        if re.search(r'\bseed', line, re.I): seeds += _parse_url_lines(line)
    if not roots and root_seed_urls: roots=[root_seed_urls[0]]
    if not seeds and len(root_seed_urls)>1: seeds=root_seed_urls[1:]
    blacklist_block=sections.get('BLACKLIST','') or ''
    if not blacklist_block:
        m=re.search(r'(?ims)^={4,}\s*\n\s*BLACKLIST\s*:?\s*\n\s*={4,}\s*\n(.*?)(?:\n\s*={4,}\s*$|\Z)', raw)
        if m:
            blacklist_block=m.group(1)
    blacklist=[]
    for ln in (blacklist_block or '').splitlines():
        x=ln.strip().strip('-•*')
        if not x or x.startswith('#') or set(x)<={'='}: continue
        blacklist.append(x)
    cfg={'type':typ,'urls':urls,'roots':roots,'seeds':seeds,'raw_root_seed_urls':root_seed_urls,
         'depth':find_int('Depth',2),'max_pages':find_int('Max Pages',100),
         'name':name,
         'pattern_expansion':find_bool('Pattern Expansion',False),'route_inference':find_bool('Route Inference',False),'soft_probe':find_bool('Soft Probe',False),'use_pattern_seeds':find_bool('Use Seeds',True),'auto_pagination':find_bool('Auto Pagination',False),'pagination_limit':find_int('Pagination Limit',25),'follow_masked_outbound':find_bool('Follow Masked Outbound',False),'masked_outbound_aggressive':find_bool('Aggressive External Resolver',False),'masked_outbound_limit':find_int('External Limit',300),'external_theme_filter':find_bool('Filter External By Theme', find_bool('External Theme Filter', False)),'external_theme_keywords':_split_csvish(find_text('Theme Keywords', find_text('External Theme Keywords',''))),'use_blacklist_dirs':find_bool('Use Blacklist Dirs', find_bool('Use Blacklist', True)),
         'directory_discovery':find_bool('Directory Discovery Mode', False),
         'known_directory_url':_first_url(find_text('Known Directory URL','')),
         'root_url':_first_url(find_text('Root URL','')) or (roots[0] if roots else (urls[0] if urls else '')),
         'section_root_url':_first_url(find_text('Section Root URL','')),
         'directory_keywords':_split_csvish(find_text('Directory Keywords','')),
         'max_sibling_routes':find_int('Max sibling routes',30),
         'find_pagination':find_bool('Find pagination', find_bool('Auto Pagination', False)),
         'find_listing_cards':find_bool('Find listing cards', True),
         'find_outbound_company_links':find_bool('Find outbound company links', find_bool('Follow Masked Outbound', False)),
         'find_profile_detail_pages':find_bool('Find profile/detail pages', True),
         'blacklist':blacklist,
         'warnings':warnings}
    if cfg['type'].lower().replace('-',' ').startswith('bulk') and not cfg['urls']: cfg['urls']=root_seed_urls
    return cfg

def _parse_config_bundle(text: str) -> list[dict]:
    configs=[]
    for idx, block in enumerate(_split_config_blocks(text)):
        cfg=_parse_config_text(block)
        cfg.setdefault('warnings', [])
        cfg['import_index']=idx+1
        if not cfg.get('name'):
            cfg['name']=f'config_{idx+1:02d}'
        configs.append(cfg)
    return configs

def _config_summary(cfg: dict) -> dict:
    return {'name':cfg.get('name') or '', 'type':cfg.get('type'), 'urls':len(cfg.get('urls') or []), 'roots':len(cfg.get('roots') or []), 'seeds':len(cfg.get('seeds') or []), 'blacklist':len(cfg.get('blacklist') or []), 'warnings':len(cfg.get('warnings') or []), 'directory_discovery':bool(cfg.get('directory_discovery')), 'targets':len(_config_targets(cfg))}

def _bundle_summary(configs: list[dict]) -> dict:
    return {'configs':len(configs), 'targets':sum(len(_config_targets(c)) for c in configs), 'urls':sum(len(c.get('urls') or []) for c in configs), 'roots':sum(len(c.get('roots') or []) for c in configs), 'seeds':sum(len(c.get('seeds') or []) for c in configs), 'blacklist':sum(len(c.get('blacklist') or []) for c in configs), 'warnings':sum(len(c.get('warnings') or []) for c in configs)}

def _config_targets(cfg: dict) -> list[tuple[str, str]]:
    typ=str(cfg.get('type') or '').lower().replace('-',' ')
    targets=[]
    if 'root' in typ and 'seed' in typ:
        targets += [('root', u) for u in (cfg.get('roots') or [])]
        targets += [('seed', u) for u in (cfg.get('seeds') or [])]
    elif 'bulk' in typ:
        targets += [('url', u) for u in (cfg.get('urls') or [])]
    else:
        targets += [('url', u) for u in ((cfg.get('urls') or cfg.get('seeds') or cfg.get('roots') or []))]
    out=[]; seen=set()
    for kind,u in targets:
        if u and u not in seen:
            seen.add(u); out.append((kind,u))
    return out

def _config_to_job_payload(cfg: dict, url: str) -> dict:
    return {'url':url,'depth':int(cfg.get('depth') or cfg.get('max_depth') or 2),'max_depth':int(cfg.get('max_depth') or cfg.get('depth') or 2),'max_pages':int(cfg.get('max_pages') or 100),'pattern_expansion':bool(cfg.get('pattern_expansion')),'route_inference':bool(cfg.get('route_inference')),'soft_probe':bool(cfg.get('soft_probe')),'use_pattern_seeds':bool(cfg.get('use_pattern_seeds',True)),'auto_pagination':bool(cfg.get('auto_pagination')),'pagination_limit':int(cfg.get('pagination_limit') or 25),'follow_masked_outbound':bool(cfg.get('follow_masked_outbound')),'masked_outbound_aggressive':bool(cfg.get('masked_outbound_aggressive')),'masked_detail_boost':bool(cfg.get('masked_outbound_aggressive')),'masked_outbound_limit':int(cfg.get('masked_outbound_limit') or 300),'external_theme_filter':bool(cfg.get('external_theme_filter')),'external_theme_keywords':cfg.get('external_theme_keywords') or [],'use_blacklist_dirs':bool(cfg.get('use_blacklist_dirs',True)),'imported_config':True,'config_name':cfg.get('name') or 'imported_config','directory_discovery':bool(cfg.get('directory_discovery')),'known_directory_url':cfg.get('known_directory_url',''),'root_url':cfg.get('root_url',''),'section_root_url':cfg.get('section_root_url','')}

def _apply_blacklist_from_config(cfg: dict) -> int:
    rows=[]
    for x in cfg.get('blacklist') or []:
        raw=str(x).strip()
        if not raw: continue
        kind='regex' if raw.startswith(('re:','regex:')) or raw.startswith('{regex') else 'contains'
        pattern=raw.split(':',1)[1].strip() if raw.startswith(('re:','regex:')) else raw
        if raw.startswith('{regex'): pattern=raw.strip('{}')
        rows.append({'pattern':pattern,'kind':kind,'enabled':True,'note':'imported from config'})
    for r in rows:
        try: add_blacklist_rule(r)
        except Exception: pass
    return len(rows)

@app.post('/api/config/parse')
def api_config_parse():
    data=request.get_json(force=True)
    if isinstance(data, dict) and isinstance(data.get('configs'), list):
        configs=data.get('configs') or []
    else:
        configs=_parse_config_bundle(data.get('text','') if isinstance(data,dict) else '')
    cfg=configs[0] if configs else {}
    return jsonify({'config':cfg,'configs':configs,'summary':_config_summary(cfg) if cfg else {},'bundle_summary':_bundle_summary(configs),'warnings':cfg.get('warnings') or []})

@app.post('/api/config/launch')
def api_config_launch():
    data=request.get_json(force=True)
    if isinstance(data.get('configs'), list):
        configs=data.get('configs') or []
    elif data.get('config'):
        configs=[data.get('config')]
    else:
        configs=_parse_config_bundle(data.get('text',''))
    apply_blacklist=data.get('apply_blacklist', True)
    batch_id=stable_id('import-config', str(time.time()))
    jobs=[]; seen=set(); blacklist_count=0
    for cfg_index, cfg in enumerate(configs, start=1):
        if apply_blacklist:
            blacklist_count += _apply_blacklist_from_config(cfg)
        for target_type, u in _config_targets(cfg):
            key=(u, cfg.get('name') or cfg_index)
            if not u or key in seen: continue
            seen.add(key)
            payload=_config_to_job_payload(cfg,u)
            payload.update({'config_batch_id':batch_id,'config_type':cfg.get('type'),'config_name':cfg.get('name') or f'config_{cfg_index:02d}','mode':'config_import'})
            if target_type == 'root': payload['root_url']=u
            if target_type == 'seed': payload['seed_url']=u
            jobs.append(start_job(payload))
    event_bus.publish('config_jobs_launched', {'batch_id':batch_id,'jobs':len(jobs),'configs':len(configs),'blacklist_rules':blacklist_count})
    return jsonify({'batch_id':batch_id,'jobs':jobs,'count':len(jobs),'configs':configs,'config':configs[0] if configs else {},'bundle_summary':_bundle_summary(configs),'blacklist_rules':blacklist_count}), 202

@app.get('/api/config/export')
def api_config_export():
    jid=request.args.get('job_id'); j=job_store.get(jid) if jid else None
    if not j: j={'url':'https://example.com/','depth':2,'max_pages':100,'pattern_expansion':True,'route_inference':False,'soft_probe':True,'use_pattern_seeds':True,'auto_pagination':False,'pagination_limit':25,'follow_masked_outbound':True,'masked_outbound_aggressive':True,'masked_outbound_limit':300}
    lines=['========','TYPE:','Single Job','========','URL:',j.get('url',''),'========','URL ROOT-SEED:',f"Root: {j.get('url','')}",f"Seed_0: {j.get('url','')}",'========','CONFIG','========',f"Depth: {j.get('max_depth') or j.get('depth') or 2}",f"Max Pages: {j.get('max_pages') or 100}",f"Pattern Expansion: {'on' if j.get('pattern_expansion') else 'off'}",f"Route Inference: {'on' if j.get('route_inference') else 'off'}",f"Soft Probe: {'on' if j.get('soft_probe') else 'off'}",f"Use Seeds: {'on' if j.get('use_pattern_seeds', True) else 'off'}",f"Auto Pagination: {'on' if j.get('auto_pagination') else 'off'}",f"Pagination Limit: {j.get('pagination_limit') or 25}",f"Follow Masked Outbound: {'on' if j.get('follow_masked_outbound') else 'off'}",f"Aggressive External Resolver: {'on' if j.get('masked_outbound_aggressive') else 'off'}",f"External Limit: {j.get('masked_outbound_limit') or 300}",f"Filter External By Theme: {'on' if j.get('external_theme_filter') else 'off'}",f"Theme Keywords: {', '.join(j.get('external_theme_keywords') or []) if isinstance(j.get('external_theme_keywords'), list) else (j.get('external_theme_keywords') or '')}",'========','BLACKLIST','========','login','blog','news','help','terms','cookies','re:/(wp-content|wp-admin|wp-includes)(/|$)','========']
    return jsonify({'text':'\n'.join(lines)})

def _dedupe(seq):
    out=[]; seen=set()
    for x in seq:
        if x and x not in seen: seen.add(x); out.append(x)
    return out

def _directory_candidates(root_url, section_root, known_url, keywords, max_sibling=20):
    from urllib.parse import urlparse, urljoin
    kws=[k.strip('/ ').lower() for k in (keywords or []) if k.strip('/ ')]
    bases=[]
    for u in [known_url, section_root, root_url]:
        if not u: continue
        p=urlparse(u)
        if not p.scheme or not p.netloc: continue
        parts=[x for x in (p.path or '/').split('/') if x]
        bases.append(f'{p.scheme}://{p.netloc}/')
        if parts:
            bases.append(f'{p.scheme}://{p.netloc}/' + '/'.join(parts[:-1]) + '/')
            bases.append(f'{p.scheme}://{p.netloc}/' + parts[0] + '/')
    bases=_dedupe(bases); candidates=[]
    for b in bases[:10]:
        candidates.append(b)
        for kw in kws[:max_sibling]: candidates.append(urljoin(b, kw.strip('/') + '/'))
    if known_url: candidates.insert(0, known_url)
    if section_root: candidates.insert(0, section_root)
    if root_url: candidates.insert(0, root_url)
    return _dedupe(candidates)[:max(10, max_sibling*4)]

@app.post('/api/directory-discovery/preview')
def api_directory_discovery_preview():
    d=request.get_json(force=True); keywords=d.get('directory_keywords') or d.get('keywords') or []
    if isinstance(keywords,str): keywords=[x.strip() for x in keywords.replace('\n',',').split(',') if x.strip()]
    c=_directory_candidates(d.get('root_url',''), d.get('section_root_url',''), d.get('known_directory_url',''), keywords, int(d.get('max_sibling_routes') or 20))
    rows=[{'url':u,'reason':'known/section/root candidate' if u in [d.get('root_url'),d.get('section_root_url'),d.get('known_directory_url')] else 'keyword sibling route'} for u in c]
    return jsonify({'candidates':rows,'count':len(rows)})

@app.post('/api/directory-discovery/run')
def api_directory_discovery_run():
    d=request.get_json(force=True)
    keywords=d.get('directory_keywords') or d.get('keywords') or []
    if isinstance(keywords,str): keywords=[x.strip() for x in keywords.replace('\n',',').split(',') if x.strip()]
    c=_directory_candidates(d.get('root_url',''), d.get('section_root_url',''), d.get('known_directory_url',''), keywords, int(d.get('max_sibling_routes') or 20))
    batch_id=stable_id('directory-discovery', str(time.time())); jobs=[]
    for u in c[:int(d.get('launch_limit') or 50)]:
        payload={'url':u,'depth':int(d.get('depth') or 2),'max_depth':int(d.get('depth') or 2),'max_pages':int(d.get('max_pages') or 100),'pattern_expansion':_truthy(d.get('pattern_expansion'),True),'route_inference':_truthy(d.get('route_inference'),True),'soft_probe':_truthy(d.get('soft_probe'),True),'use_pattern_seeds':_truthy(d.get('use_pattern_seeds'),True),'auto_pagination':_truthy(d.get('find_pagination'),True),'pagination_limit':int(d.get('pagination_limit') or 25),'follow_masked_outbound':_truthy(d.get('find_outbound_company_links'),False),'masked_outbound_aggressive':_truthy(d.get('aggressive_external_resolver'),False),'masked_outbound_limit':int(d.get('external_limit') or 300),'external_theme_filter':_truthy(d.get('external_theme_filter'),False),'external_theme_keywords':d.get('external_theme_keywords') or d.get('theme_keywords') or [],'directory_discovery':True,'directory_discovery_batch_id':batch_id}
        jobs.append(start_job(payload))
    event_bus.publish('directory_discovery_launched', {'batch_id':batch_id,'jobs':len(jobs),'candidates':len(c)})
    return jsonify({'batch_id':batch_id,'jobs':jobs,'count':len(jobs),'candidates':[{'url':u} for u in c]}), 202

@app.get('/api/optimization')
def api_optimization_get():
    """Return the current runtime optimization config."""
    cfg = runtime_config.get_all()
    return jsonify({"config": cfg, "defaults": runtime_config.DEFAULTS, "enabled": bool(cfg.get("optimization_enabled"))})


@app.post('/api/optimization')
def api_optimization_set():
    """Toggle optimization mode and/or update tunable values.

    Body accepts any subset of the keys in ``runtime_config.DEFAULTS``. Unknown
    keys are silently ignored so the dashboard can be forward-compatible.
    """
    data = request.get_json(force=True, silent=True) or {}
    # Accept both {"enabled": true} and {"optimization_enabled": true} from
    # different UI clients.
    if "enabled" in data and "optimization_enabled" not in data:
        data["optimization_enabled"] = bool(data.pop("enabled"))
    cfg = runtime_config.update(data)
    notify_optimization_changed()
    # Re-apply Windows priority if the toggle just flipped on a Windows host.
    prio = apply_windows_priority_once()
    event_bus.publish('optimization_updated', {"config": cfg, "windows_priority": prio})
    return jsonify({"config": cfg, "enabled": bool(cfg.get("optimization_enabled")), "windows_priority": prio})


@app.post('/api/optimization/reset')
def api_optimization_reset():
    cfg = runtime_config.reset()
    notify_optimization_changed()
    event_bus.publish('optimization_reset', {"config": cfg})
    return jsonify({"config": cfg, "enabled": bool(cfg.get("optimization_enabled"))})


if __name__ == "__main__":
    # Apply Windows process priority (BELOW_NORMAL) before serving traffic so
    # the desktop stays responsive even on a saturated crawl. No-op on Linux.
    apply_windows_priority_once()
    port = int(os.environ.get("WISP_PORT", "5220"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
