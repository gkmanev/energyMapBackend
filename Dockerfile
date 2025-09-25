# -------- builder --------
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc libpq-dev \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp/build
RUN python -m pip install --upgrade pip wheel setuptools
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /tmp/wheels -r requirements.txt

# -------- runtime --------
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH="/usr/local/bin:${PATH}"
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
  && rm -rf /var/lib/apt/lists/*
RUN useradd -m appuser
ENV HOME=/home/app
ENV APP_HOME=/home/app/web
RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME
COPY --from=builder /tmp/wheels /wheels
COPY --from=builder /tmp/build/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir /wheels/*
COPY . $APP_HOME
RUN chown -R appuser:appuser $APP_HOME
USER appuser
CMD ["python", "-m", "http.server", "8000"]
