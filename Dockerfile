FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

# Create non-root user (uid 10014) to satisfy CKV_DOCKER_3
RUN groupadd -g 10014 appuser && \
    useradd -u 10014 -g 10014 -m -d /home/appuser appuser

WORKDIR /app

# Install deps as root (ok)
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Ensure writable directories for non-root user
RUN mkdir -p /app/data /app/data/projects /app/data/tmp && \
    chown -R appuser:appuser /app

# Run as non-root
USER 10014

EXPOSE 8080
CMD ["python", "hosting_bot.py"]
