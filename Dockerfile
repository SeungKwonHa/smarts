FROM python:3.12-slim

WORKDIR /app

# System deps for asyncpg, Pillow, Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"

COPY . .

# Playwright browsers (only needed in workers that scrape)
RUN python -m playwright install chromium --with-deps 2>/dev/null || true

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "relay.apps.web.main"]
