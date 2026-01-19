FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    DATA_DIR=/tmp/hostingbot

RUN groupadd -g 10014 appuser && \
    useradd -u 10014 -g 10014 -m -d /home/appuser appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Make sure writable paths exist for non-root
RUN mkdir -p /tmp/hostingbot && \
    chown -R appuser:appuser /tmp/hostingbot

USER 10014

EXPOSE 8080
CMD ["python", "hosting_bot.py"]
