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
from storage.blacklist_store import list_rules as list_blacklist_rules, add_rule as add_blacklist_rule, update_rule as update_blacklist_rule, delete_rule as delete_blacklist_rule, replace_rules as replace_blacklist_rules
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

def _parse_config_text(text: str) -> dict:
    import json, re
    raw = (text or '').replace('\r\n','\n').replace('\r','\n')
    try:
        if raw.strip().startswith('{'):
            d=json.loads(raw)
            if isinstance(d, dict): return d
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
    def find_bool(name, default=False):
        m=re.search(rf"{re.escape(name)}\s*:\s*([^\n]+)", body, re.I)
        return _truthy(m.group(1), default) if m else default
    def find_int(name, default):
        m=re.search(rf"{re.escape(name)}\s*:\s*([^\n]+)", body, re.I)
        return _first_int(m.group(1), default) if m else default
    typ=(sections.get('TYPE') or '').strip().splitlines()[0:1]
    typ=typ[0].strip() if typ else 'Single Job'
    urls=_parse_url_lines(sections.get('URL',''))
    root_seed_block=sections.get('URL ROOT-SEED','') or sections.get('ROOT-SEED','') or sections.get('ROOT-SEED URL','')
    root_seed_urls=_parse_url_lines(root_seed_block)
    roots=[]; seeds=[]
    for line in root_seed_block.splitlines():
        if re.search(r'\broot\b', line, re.I): roots += _parse_url_lines(line)
        if re.search(r'\bseed', line, re.I): seeds += _parse_url_lines(line)
    if not roots and root_seed_urls: roots=[root_seed_urls[0]]
    if not seeds and len(root_seed_urls)>1: seeds=root_seed_urls[1:]
    blacklist=[]
    for ln in (sections.get('BLACKLIST','') or '').splitlines():
        x=ln.strip().strip('-•*')
        if not x or x.startswith('#') or set(x)<={'='}: continue
        blacklist.append(x)
    cfg={'type':typ,'urls':urls,'roots':roots,'seeds':seeds,'raw_root_seed_urls':root_seed_urls,
         'depth':find_int('Depth',2),'max_pages':find_int('Max Pages',100),
         'pattern_expansion':find_bool('Pattern Expansion',False),'route_inference':find_bool('Route Inference',False),'soft_probe':find_bool('Soft Probe',False),'use_pattern_seeds':find_bool('Use Seeds',True),'auto_pagination':find_bool('Auto Pagination',False),'pagination_limit':find_int('Pagination Limit',25),'follow_masked_outbound':find_bool('Follow Masked Outbound',False),'masked_outbound_aggressive':find_bool('Aggressive External Resolver',False),'masked_outbound_limit':find_int('External Limit',300),'use_blacklist_dirs':True,'blacklist':blacklist}
    if cfg['type'].lower().replace('-',' ').startswith('bulk') and not cfg['urls']: cfg['urls']=root_seed_urls
    return cfg

def _config_to_job_payload(cfg: dict, url: str) -> dict:
    return {'url':url,'depth':int(cfg.get('depth') or cfg.get('max_depth') or 2),'max_depth':int(cfg.get('max_depth') or cfg.get('depth') or 2),'max_pages':int(cfg.get('max_pages') or 100),'pattern_expansion':bool(cfg.get('pattern_expansion')),'route_inference':bool(cfg.get('route_inference')),'soft_probe':bool(cfg.get('soft_probe')),'use_pattern_seeds':bool(cfg.get('use_pattern_seeds',True)),'auto_pagination':bool(cfg.get('auto_pagination')),'pagination_limit':int(cfg.get('pagination_limit') or 25),'follow_masked_outbound':bool(cfg.get('follow_masked_outbound')),'masked_outbound_aggressive':bool(cfg.get('masked_outbound_aggressive')),'masked_detail_boost':bool(cfg.get('masked_outbound_aggressive')),'masked_outbound_limit':int(cfg.get('masked_outbound_limit') or 300),'use_blacklist_dirs':bool(cfg.get('use_blacklist_dirs',True)),'imported_config':True}

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
    data=request.get_json(force=True); cfg=_parse_config_text(data.get('text','') if isinstance(data,dict) else '')
    return jsonify({'config':cfg,'summary':{'type':cfg.get('type'),'urls':len(cfg.get('urls') or []),'roots':len(cfg.get('roots') or []),'seeds':len(cfg.get('seeds') or []),'blacklist':len(cfg.get('blacklist') or [])}})

@app.post('/api/config/launch')
def api_config_launch():
    data=request.get_json(force=True); cfg=data.get('config') or _parse_config_text(data.get('text',''))
    if data.get('apply_blacklist', True): _apply_blacklist_from_config(cfg)
    typ=str(cfg.get('type') or '').lower().replace('-',' ')
    if 'root' in typ and 'seed' in typ: targets=(cfg.get('roots') or [])+(cfg.get('seeds') or [])
    elif 'bulk' in typ: targets=cfg.get('urls') or []
    else: targets=cfg.get('urls') or cfg.get('seeds') or cfg.get('roots') or []
    seen=set(); jobs=[]; batch_id=stable_id('import-config', str(time.time()))
    for u in targets:
        if not u or u in seen: continue
        seen.add(u); payload=_config_to_job_payload(cfg,u); payload.update({'config_batch_id':batch_id,'config_type':cfg.get('type')}); jobs.append(start_job(payload))
    event_bus.publish('config_jobs_launched', {'batch_id':batch_id,'jobs':len(jobs),'type':cfg.get('type')})
    return jsonify({'batch_id':batch_id,'jobs':jobs,'count':len(jobs),'config':cfg}), 202

@app.get('/api/config/export')
def api_config_export():
    jid=request.args.get('job_id'); j=job_store.get(jid) if jid else None
    if not j: j={'url':'https://example.com/','depth':2,'max_pages':100,'pattern_expansion':True,'route_inference':False,'soft_probe':True,'use_pattern_seeds':True,'auto_pagination':False,'pagination_limit':25,'follow_masked_outbound':True,'masked_outbound_aggressive':True,'masked_outbound_limit':300}
    lines=['========','TYPE:','Single Job','========','URL:',j.get('url',''),'========','URL ROOT-SEED:',f"Root: {j.get('url','')}",f"Seed_0: {j.get('url','')}",'========','CONFIG','========',f"Depth: {j.get('max_depth') or j.get('depth') or 2}",f"Max Pages: {j.get('max_pages') or 100}",f"Pattern Expansion: {'on' if j.get('pattern_expansion') else 'off'}",f"Route Inference: {'on' if j.get('route_inference') else 'off'}",f"Soft Probe: {'on' if j.get('soft_probe') else 'off'}",f"Use Seeds: {'on' if j.get('use_pattern_seeds', True) else 'off'}",f"Auto Pagination: {'on' if j.get('auto_pagination') else 'off'}",f"Pagination Limit: {j.get('pagination_limit') or 25}",f"Follow Masked Outbound: {'on' if j.get('follow_masked_outbound') else 'off'}",f"Aggressive External Resolver: {'on' if j.get('masked_outbound_aggressive') else 'off'}",f"External Limit: {j.get('masked_outbound_limit') or 300}",'========','BLACKLIST','========','login','blog','news','help','terms','cookies','re:/(wp-content|wp-admin|wp-includes)(/|$)','========']
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
        payload={'url':u,'depth':int(d.get('depth') or 2),'max_depth':int(d.get('depth') or 2),'max_pages':int(d.get('max_pages') or 100),'pattern_expansion':_truthy(d.get('pattern_expansion'),True),'route_inference':_truthy(d.get('route_inference'),True),'soft_probe':_truthy(d.get('soft_probe'),True),'use_pattern_seeds':_truthy(d.get('use_pattern_seeds'),True),'auto_pagination':_truthy(d.get('find_pagination'),True),'pagination_limit':int(d.get('pagination_limit') or 25),'follow_masked_outbound':_truthy(d.get('find_outbound_company_links'),False),'masked_outbound_aggressive':_truthy(d.get('aggressive_external_resolver'),False),'masked_outbound_limit':int(d.get('external_limit') or 300),'directory_discovery':True,'directory_discovery_batch_id':batch_id}
        jobs.append(start_job(payload))
    event_bus.publish('directory_discovery_launched', {'batch_id':batch_id,'jobs':len(jobs),'candidates':len(c)})
    return jsonify({'batch_id':batch_id,'jobs':jobs,'count':len(jobs),'candidates':[{'url':u} for u in c]}), 202

if __name__ == "__main__":
    port = int(os.environ.get("WISP_PORT", "5220"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
