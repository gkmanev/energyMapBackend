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

# Install system deps + Chrome for Selenium scraping
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 curl gnupg xvfb \
  && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
      | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] \
      http://dl.google.com/linux/chrome/deb/ stable main" \
      > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
  && apt-get purge -y curl gnupg && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools

RUN useradd -m appuser
ENV HOME=/home/appuser
ENV APP_HOME=/home/appuser/web
RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME
COPY --from=builder /tmp/wheels /wheels
COPY --from=builder /tmp/build/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir /wheels/*
COPY . $APP_HOME
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown -R appuser:appuser /home/appuser
USER appuser
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "http.server", "8000"]
