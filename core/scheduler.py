from __future__ import annotations
import threading, asyncio, time
from utils.hashing import stable_id
from storage.job_store import job_store
from storage.pattern_store import pattern_store
from storage.service_store import service_store
from storage.checkpoint_store import checkpoint_store
from graph.graph_store import graph_store
from core.crawler import CrawlEngine, JobCancelled, JobRestartRequested
from storage.event_bus import event_bus
from utils.urls import domain_of

RUNNING: dict[str, threading.Thread] = {}

def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _job_elapsed(job: dict) -> int:
    started = float(job.get("started_unix") or job.get("created_unix") or time.time())
    paused_total = float(job.get("paused_total_seconds") or 0)
    if job.get("status") == "paused" and job.get("paused_at_unix"):
        paused_total += max(0, time.time() - float(job.get("paused_at_unix") or time.time()))
    return int(max(0, time.time() - started - paused_total))

def enrich_job(job: dict | None) -> dict | None:
    if not job:
        return job
    job = dict(job)
    job["is_thread_alive"] = bool(job.get("id") in RUNNING and RUNNING[job.get("id")].is_alive())
    if job.get("status") in ("running", "paused", "pausing", "restarting", "pause_requested", "cancel_requested"):
        job["elapsed_seconds"] = _job_elapsed(job)
        hb = job.get("last_heartbeat_unix") or job.get("started_unix") or job.get("created_unix")
        try:
            job["seconds_since_heartbeat"] = int(max(0, time.time() - float(hb)))
        except Exception:
            job["seconds_since_heartbeat"] = None
    try:
        cp = checkpoint_store.get(job.get("id"))
        if cp:
            job["checkpoint"] = {"saved_at": cp.get("saved_at"), "pages": cp.get("pages", 0), "queue": cp.get("queue_size", len(cp.get("queue", []))), "current_url": cp.get("current_url")}
    except Exception:
        pass
    return job

def list_jobs():
    return [enrich_job(j) for j in job_store.list()]

def get_job(jid: str):
    return enrich_job(job_store.get(jid))

def start_job(payload: dict, rerun_of: str | None = None) -> dict:
    jid = stable_id(payload.get("url",""), str(time.time()))
    now = time.time()
    job = {
        "id": jid, "url": payload["url"], "depth": int(payload.get("depth", payload.get("max_depth", 2))),
        "max_pages": int(payload.get("max_pages", payload.get("pages", 200))), "status": "queued",
        "progress": {"pages": 0},
        "options": {"pattern_expansion": payload.get("pattern_expansion", True), "route_inference": payload.get("route_inference", True), "api_discovery": payload.get("api_discovery", True), "hypothetical_urls": payload.get("hypothetical_urls", True), "use_pattern_seeds": payload.get("use_pattern_seeds", True), "use_suggested_values": payload.get("use_suggested_values", True), "soft_probe": payload.get("soft_probe", True), "expand_existing_graph": payload.get("expand_existing_graph", True), "follow_masked_outbound": payload.get("follow_masked_outbound", False), "masked_outbound_aggressive": payload.get("masked_outbound_aggressive", False), "masked_detail_boost": payload.get("masked_detail_boost", payload.get("masked_outbound_aggressive", False)), "masked_outbound_limit": int(payload.get("masked_outbound_limit", 500 if payload.get("masked_outbound_aggressive", False) else 120)), "external_resolver_timeout": int(payload.get("external_resolver_timeout", 22 if payload.get("masked_outbound_aggressive", False) else 15)), "auto_pagination": payload.get("auto_pagination", False), "pagination_limit": int(payload.get("pagination_limit", 25))},
        "rerun_of": rerun_of, "mode": "deep_crawl" if rerun_of else payload.get("mode", "crawl"),
        "created_at": _now_iso(), "created_unix": now, "started_unix": None, "elapsed_seconds": 0,
        "paused_total_seconds": 0, "control": "running", "resume_from_checkpoint": False
    }
    job_store.put(jid, job)
    event_bus.publish("job_queued", {"job_id": jid, "url": job["url"], "status": "queued", "options": job.get("options", {})})
    _start_thread(jid)
    return enrich_job(job_store.get(jid))

def _start_thread(jid: str):
    old = RUNNING.get(jid)
    if old and old.is_alive():
        return old
    t = threading.Thread(target=_run_thread, args=(jid,), daemon=True)
    RUNNING[jid] = t
    t.start()
    return t

def _persist_result(jid: str, job: dict, result: dict):
    graph_store.put(jid, {"id": jid, **result["graph"], "job_id": jid, "skipped": result.get("skipped", []), "external_report": result.get("external_report", {}), "pagination_report": result.get("pagination_report", {}), "checkpoint": result.get("checkpoint")})
    for p in result.get("patterns", []):
        pid = stable_id(p.get("domain",""), p.get("pattern",""))
        existing = pattern_store.get(pid) or {}
        existing.update({**p, "id": pid, "job_id": jid})
        pattern_store.put(pid, existing)
    service_id = stable_id(domain_of(job["url"]))
    service_store.put(service_id, {"id": service_id, "domain": domain_of(job["url"]), "url": job["url"], "last_job_id": jid, "node_count": len(result["graph"]["nodes"]), "edge_count": len(result["graph"]["edges"]), "updated_at": _now_iso()})

def _run_thread(jid: str):
    job = job_store.get(jid)
    if not job: return
    try:
        job["status"] = "running"
        job["control"] = "running"
        job.setdefault("started_unix", time.time())
        if not job.get("started_unix"):
            job["started_unix"] = time.time()
        job["started_at"] = job.get("started_at") or _now_iso()
        job["last_heartbeat"] = _now_iso()
        job["last_heartbeat_unix"] = time.time()
        job_store.put(jid, job)
        event_bus.publish("job_started", {"job_id": jid, "url": job.get("url"), "status": "running", "resume_from_checkpoint": bool(job.get("resume_from_checkpoint"))})
        event_bus.publish("job_progress", {"job_id": jid, "status": "running", "elapsed_seconds": _job_elapsed(job)})
        result = asyncio.run(CrawlEngine(job).run())
        job = job_store.get(jid) or job
        _persist_result(jid, job, result)
        ext = result.get("external_report", {})
        job["status"] = "completed"
        job["control"] = "completed"
        job["finished_at"] = _now_iso()
        job["finished_unix"] = time.time()
        job["elapsed_seconds"] = _job_elapsed(job)
        job["progress"] = {"pages": result.get("pages",0), "nodes": len(result["graph"]["nodes"]), "edges": len(result["graph"]["edges"]), "skipped": len(result.get("skipped", [])), "external_candidates": len(ext.get("candidates", [])), "external_resolved": len(ext.get("resolved", [])), "external_failed": len(ext.get("failed", [])), "pagination_pages": len((result.get("pagination_report") or {}).get("pages", [])), "pagination_links": (result.get("pagination_report") or {}).get("links_found", 0)}
        job_store.put(jid, job)
        event_bus.publish("job_completed", enrich_job(job))
    except JobRestartRequested:
        job = job_store.get(jid) or job
        job["status"] = "restarting"
        job["control"] = "running"
        job["resume_from_checkpoint"] = True
        job["restart_count"] = int(job.get("restart_count", 0) or 0) + 1
        job["last_restart_at"] = _now_iso()
        job_store.put(jid, job)
        event_bus.publish("job_restarting", {"job_id": jid, "restart_count": job["restart_count"], "checkpoint": job.get("checkpoint")})
        RUNNING.pop(jid, None)
        _start_thread(jid)
        return
    except JobCancelled:
        job = job_store.get(jid) or job
        job["status"] = "cancelled"
        job["control"] = "cancelled"
        job["finished_at"] = _now_iso()
        job["finished_unix"] = time.time()
        job["elapsed_seconds"] = _job_elapsed(job)
        job_store.put(jid, job)
        event_bus.publish("job_cancelled", {"job_id": jid, "elapsed_seconds": job.get("elapsed_seconds"), "checkpoint": job.get("checkpoint")})
    except Exception as e:
        job = job_store.get(jid) or job
        job["status"] = "failed"
        job["control"] = "failed"
        job["error"] = str(e)
        job["finished_at"] = _now_iso()
        job["finished_unix"] = time.time()
        job["elapsed_seconds"] = _job_elapsed(job)
        job_store.put(jid, job)
        event_bus.publish("job_failed", {"job_id": jid, "error": str(e), "elapsed_seconds": job.get("elapsed_seconds")})
    finally:
        RUNNING.pop(jid, None)

def pause_job(jid: str):
    job = job_store.get(jid)
    if not job: return None
    job["status"] = "pause_requested"
    job["control"] = "pause"
    job_store.put(jid, job)
    event_bus.publish("job_pause_requested", {"job_id": jid})
    return enrich_job(job)

def resume_job(jid: str):
    job = job_store.get(jid)
    if not job: return None
    if jid not in RUNNING or not RUNNING[jid].is_alive():
        job["resume_from_checkpoint"] = True
        job["control"] = "running"
        job["status"] = "queued"
        job_store.put(jid, job)
        _start_thread(jid)
    else:
        job["control"] = "resume"
        job["status"] = "resuming"
        job_store.put(jid, job)
    event_bus.publish("job_resume_requested", {"job_id": jid})
    return enrich_job(job_store.get(jid))

def restart_job_from_checkpoint(jid: str):
    job = job_store.get(jid)
    if not job: return None
    job["resume_from_checkpoint"] = True
    job["restart_requested_at"] = _now_iso()
    if jid in RUNNING and RUNNING[jid].is_alive():
        job["control"] = "restart_requested"
        job["status"] = "restarting"
        job_store.put(jid, job)
        event_bus.publish("job_restart_requested", {"job_id": jid, "mode": "cooperative"})
    else:
        job["control"] = "running"
        job["status"] = "queued"
        job_store.put(jid, job)
        _start_thread(jid)
        event_bus.publish("job_restart_requested", {"job_id": jid, "mode": "from_checkpoint"})
    return enrich_job(job_store.get(jid))

def cancel_job(jid: str):
    job = job_store.get(jid)
    if not job: return None
    job["control"] = "cancel"
    job["status"] = "cancel_requested"
    job_store.put(jid, job)
    event_bus.publish("job_cancel_requested", {"job_id": jid})
    return enrich_job(job)

def delete_job(jid: str):
    job = job_store.get(jid)
    if not job: return {"deleted": False, "id": jid, "error": "job not found"}
    if jid in RUNNING and RUNNING[jid].is_alive():
        job["control"] = "delete"
        job["status"] = "deleting"
        job_store.put(jid, job)
    ok_job = job_store.delete(jid)
    ok_graph = graph_store.delete(jid)
    ok_checkpoint = checkpoint_store.delete(jid)
    event_bus.publish("job_deleted", {"job_id": jid, "url": job.get("url"), "job_deleted": ok_job, "graph_deleted": ok_graph, "checkpoint_deleted": ok_checkpoint})
    return {"deleted": ok_job, "graph_deleted": ok_graph, "checkpoint_deleted": ok_checkpoint, "id": jid}
