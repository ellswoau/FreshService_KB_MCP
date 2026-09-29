# freshservice-kb - Python 3.12 slim image.
# Contains both the pipeline (fskb) and the retrieval MCP server (fskb-mcp).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN groupadd -r fskb && useradd -r -g fskb -d /app fskb

# Install deps first for layer caching. mcp extra pulls fastmcp.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[mcp]" && chmod -R a+rX /app

COPY scripts ./scripts
RUN chmod -R a+rX scripts

# Persist the incremental watermark outside the image layer.
RUN mkdir -p /data && chown fskb:fskb /data
ENV STATE_DIR=/data/.state

USER fskb

# MCP server port (network transport). The scheduler does not bind a port.
EXPOSE 8100

# Default: long-running 4-hourly updater. Override to run the MCP server:
#   docker run -p 8100:8100 freshservice-kb fskb-mcp --transport http --port 8100
CMD ["python", "scripts/scheduler.py"]

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import os,sys; sys.exit(0 if os.path.exists(os.environ.get('STATE_DIR','/data/.state')+'/state.json') else 1)"
