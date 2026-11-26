"""Sentinel API application entry point."""

from fastapi import FastAPI

app = FastAPI(
    title="Sentinel Banking Intelligence API",
    description=(
        "Downstream fraud detection and AML monitoring platform "
        "for a simulated banking environment."
    ),
    version="0.1.0",
)


@app.get("/health", tags=["Operations"])
async def health_check() -> dict[str, str]:
    """Return basic process health; dependency health is added in a later task."""
    return {"status": "ok", "service": "sentinel-api", "version": "0.1.0"}
