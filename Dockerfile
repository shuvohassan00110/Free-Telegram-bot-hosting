FROM python:3.11-slim

# --- Runtime env ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    DATA_DIR=/tmp/hostingbot

# --- Create non-root user (Checkov CKV_DOCKER_3) ---
RUN groupadd -g 10014 appuser && \
    useradd -u 10014 -g 10014 -m -d /home/appuser appuser

WORKDIR /app

# --- Install dependencies ---
COPY requirements.txt /app/requirements.txt

# Force-install webhook deps (prevents "start_webhook requires [webhooks]" error)
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir "python-telegram-bot[webhooks]>=21.6" "tornado>=6.4.1" && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    python -c "import telegram, tornado; print('PTB=', telegram.__version__, 'TORNADO=', tornado.version)"

# --- Copy code ---
COPY . /app

# --- Writable dir for SQLite/logs/projects on Choreo ---
RUN mkdir -p /tmp/hostingbot && \
    chown -R appuser:appuser /tmp/hostingbot

# Run as non-root
USER 10014

EXPOSE 8080

CMD ["python", "hosting_bot.py"]
