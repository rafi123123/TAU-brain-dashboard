import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException

from folder_audit import audit

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
