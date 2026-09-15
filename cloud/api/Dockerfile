FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Chromium for market_fetcher.py's oree.com.ua scrape. --with-deps pulls in
# the OS-level libraries Chromium needs on this slim base image.
RUN playwright install --with-deps chromium

COPY . .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# A single worker only: the MQTT subscriber + InfluxDB writer is a stateful
# singleton background task started in the FastAPI lifespan. With
# --workers > 1, uvicorn forks multiple processes that each start their own
# subscriber using the same hardcoded MQTT client id ("cloud-api"), so the
# workers perpetually kick each other off the broker (visible as
# "[code:128] Unspecified error" disconnects with growing backoff).
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
