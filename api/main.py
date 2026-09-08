import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, BackgroundTasks
from cron import update_db
from folder_audit import audit
from fastapi.responses import HTMLResponse


# Grafana may refresh more often than the 30 minute schedule, so results are reused for a while.
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "20"))

app = FastAPI(title="Missing Folders API")

_cache: dict | None = None
_cached_at = 0.0


def cached_audit() -> dict:
    """Returns the previous audit while it is younger than CACHE_TTL_SECONDS, otherwise re-runs it,
    so repeated dashboard refreshes do not hit the NAS and the sheet every time."""
    global _cache, _cached_at
    if _cache is None or time.monotonic() - _cached_at > CACHE_TTL_SECONDS:
        _cache = audit() | {"checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        _cached_at = time.monotonic()
    return _cache


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/missing-folders")
def missing_folders():
    try:
        return cached_audit()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"{type(exc).__name__}: {exc}")


@app.get("/missing-folders/list")
def missing_folders_list():
    """Same data as a flat array of rows, which the Infinity table panel can consume without a selector."""
    return [{"folder": name} for name in missing_folders()["missing"]]

@app.get("/update", response_class=HTMLResponse)
async def update_db_endpoint(background_tasks: BackgroundTasks):
    "updating the DB manually"
    with open("/tmp/test.log", "a") as f:
        f.write("starting run_in_threadpool\n")
    background_tasks.add_task(update_db)
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Status</title>
        <style>
            body { font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #0f172a; color: #f8fafc; }
            .card { background: #1e293b; padding: 2rem 3rem; border-radius: 1rem; box-shadow: 0 10px 25px rgba(0,0,0,0.3); text-align: center; border: 1px solid #334155; }
            .dot { height: 12px; width: 12px; background-color: #38bdf8; border-radius: 50%; display: inline-block; animation: pulse 1.5s infinite; margin-right: 8px; }
            @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }
        </style>
    </head>
    <body>
        <div class="card">
            <h2><span class="dot"></span>Currently updating the DB...</h2>
        </div>
    </body>
    </html>
    """
