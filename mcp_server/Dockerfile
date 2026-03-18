FROM python:3.12-slim

WORKDIR /app

# Copy workspace files needed for uv workspace install
COPY pyproject.toml uv.lock ./
COPY lib/ lib/
COPY mcp_server/ mcp_server/

RUN pip install uv && uv sync --package crucible-mcp --no-dev

ENV TRANSPORT=sse
ENV SSE_HOST=0.0.0.0
ENV SSE_PORT=8001

EXPOSE 8001

CMD ["uv", "run", "--package", "crucible-mcp", "python", "-m", "crucible_mcp.server"]
