from fastapi import FastAPI

from .routers import fixtures, test_runs

app = FastAPI(title="Crucible Control Plane", version="0.1.0")

app.include_router(test_runs.router)
app.include_router(fixtures.router)


@app.get("/health", tags=["ops"])
async def health() -> dict:
    return {"status": "ok"}
