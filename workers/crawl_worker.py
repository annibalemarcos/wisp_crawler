from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from storage.job_store import job_store
from core.scheduler import start_job

if __name__ == "__main__":
    queued = [j for j in job_store.list() if j.get("status") == "queued"]
    for job in queued:
        start_job(job)
    print(f"WISP worker checked {len(queued)} queued jobs.")
