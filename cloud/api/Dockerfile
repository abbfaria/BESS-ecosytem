FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Build context is the repo root (see cloud/docker-compose.yml).
COPY cloud/api/requirements.txt .
RUN pip install -r requirements.txt

COPY cloud/api/src ./src
# The edge's model is the single source of truth for prices, dispatch and
# telemetry; copied (not duplicated in git) so both sides always match.
COPY edge/src/bess_model.py ./src/bess_model.py

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
