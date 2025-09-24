# -------- builder --------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps needed to compile common wheels (psycopg2, lxml, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/build

# Keep pip modern for better resolver/cache
RUN python -m pip install --upgrade pip wheel setuptools

# Install deps into wheels layer for better caching
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /tmp/wheels -r requirements.txt


# -------- runtime --------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # ensures "python" finds venv/site-packages first (optional)
    PATH="/usr/local/bin:${PATH}"

# Runtime libs only (psycopg2 needs libpq at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
  && rm -rf /var/lib/apt/lists/*

# Create non-root user & app dir
RUN useradd -m appuser
WORKDIR /app

# Install wheels built in the builder stage
COPY --from=builder /tmp/wheels /wheels
COPY --from=builder /tmp/build/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir /wheels/*

# Copy project (last, so code changes don’t bust dependency cache)
COPY . /app

# Make sure the non-root user owns the app
RUN chown -R appuser:appuser /app
USER appuser

# Default CMD is harmless—Compose will override per service
# (web service will run gunicorn from docker-compose.yml)
CMD ["python", "-m", "http.server", "8000"]
