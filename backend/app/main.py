"""Sentinel API application entry point and local simulation controls."""

from fastapi import FastAPI, HTTPException

from app.config import get_transactions_per_second
from app.db.database import create_db_engine, create_session_factory
from app.services.live_banking_engine import LiveBankingEngine

app = FastAPI(
    title="Sentinel Banking Intelligence API",
    description=(
        "Downstream fraud detection and AML monitoring platform "
        "for a simulated banking environment."
    ),
    version="0.2.0",
)

_live_engine: LiveBankingEngine | None = None


def _get_live_engine() -> LiveBankingEngine:
    """Create one process-local worker controller on first use."""
    global _live_engine
    if _live_engine is None:
        database_engine = create_db_engine()
        _live_engine = LiveBankingEngine(
            create_session_factory(database_engine),
            transactions_per_second=get_transactions_per_second(),
        )
    return _live_engine


@app.get("/health", tags=["Operations"])
async def health_check() -> dict[str, str]:
    """Return basic process health; dependency health is added in a later task."""
    return {"status": "ok", "service": "sentinel-api", "version": "0.2.0"}


@app.get("/simulation/status", tags=["Simulation"])
async def simulation_status() -> dict[str, object]:
    """Read the local synthetic banking worker state."""
    if _live_engine is None:
        return {
            "state": "stopped",
            "transactions_per_second": get_transactions_per_second(),
            "generated_transactions": 0,
            "last_error": None,
        }
    return _live_engine.status()


@app.post("/simulation/start", tags=["Simulation"])
async def start_simulation() -> dict[str, object]:
    """Start continuous transaction generation in a background thread."""
    try:
        return _get_live_engine().start()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/simulation/pause", tags=["Simulation"])
async def pause_simulation() -> dict[str, object]:
    """Pause the running generator without terminating its worker thread."""
    if _live_engine is None:
        raise HTTPException(status_code=409, detail="engine is not running")
    try:
        return _live_engine.pause()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/simulation/resume", tags=["Simulation"])
async def resume_simulation() -> dict[str, object]:
    """Resume a paused generator."""
    if _live_engine is None:
        raise HTTPException(status_code=409, detail="engine is not running")
    try:
        return _live_engine.resume()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/simulation/stop", tags=["Simulation"])
async def stop_simulation() -> dict[str, object]:
    """Stop the worker and wait for its current database operation to finish."""
    if _live_engine is None:
        return {
            "state": "stopped",
            "transactions_per_second": get_transactions_per_second(),
            "generated_transactions": 0,
            "last_error": None,
        }
    try:
        return _live_engine.stop()
    except TimeoutError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
