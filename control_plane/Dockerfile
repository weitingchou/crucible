# ── Stage 1: install dependencies ────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Copy workspace root and all packages needed to resolve the dependency graph.
# lib/ is copied because control-plane depends on crucible-lib (workspace dep).
COPY pyproject.toml .
COPY lib/ lib/
COPY control_plane/ control_plane/

# Install only the control-plane package and its transitive deps; no dev extras.
# --no-install-workspace avoids re-installing all workspace members.
RUN uv sync --package crucible-control-plane --no-dev

# ── Stage 2: lean runtime image ───────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Copy the virtualenv and source trees from the builder.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/lib /app/lib
COPY --from=builder /app/control_plane /app/control_plane

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "control_plane.main:app", "--host", "0.0.0.0", "--port", "8000"]
