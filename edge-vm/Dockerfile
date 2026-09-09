FROM python:3.11-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System dependencies: CBC solver (for PuLP MILP), build tools.
# libcoinor-cbc-dev isn't needed — PuLP shells out to the `cbc` binary from
# coinor-cbc, it doesn't link against libCbc — and the -dev package doesn't
# exist on Debian 13/trixie anyway (present on bookworm, dropped since).
RUN apt-get update && apt-get install -y --no-install-recommends \
        coinor-cbc \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Dependency layer (cached unless requirements.txt changes) ─────────────
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# ── Application layer ─────────────────────────────────────────────────────
COPY . .

# Persist DB and certs via volumes (mounted at runtime)
RUN mkdir -p /data /certs

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["python", "-m", "src.main"]
