FROM python:3.11.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY simulator ./simulator
COPY tools ./tools
COPY schemas ./schemas
COPY config ./config
COPY tests ./tests
RUN pip install --no-cache-dir ".[bootstrap,dev]"
ENTRYPOINT ["python"]
