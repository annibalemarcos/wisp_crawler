from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import os, time
from pathlib import Path
from flask import Flask, jsonify, request, Response, render_template
from flask_cors import CORS
from utils.hashing import stable_id
from storage.job_store import job_store
from storage.pattern_store import pattern_store, seed_store
from storage.service_store import service_store
from graph.graph_store import graph_store
from core.scheduler import start_job, list_jobs, get_job, pause_job, resume_job, restart_job_from_checkpoint, cancel_job, delete_job
from patterns.suggest import suggest_from_payload
from patterns.seeds import create_seed
from storage.event_bus import event_bus

ROOT = Path(__file__).resolve().parents[1]
app = Flask(__name__, template_folder=str(ROOT / "dashboard" / "templates"), static_folder=str(ROOT / "dashboard" / "static"))
CORS(app)


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

if __name__ == "__main__":
    port = int(os.environ.get("WISP_PORT", "5220"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
