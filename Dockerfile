FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 加上 ffmpeg 的安裝
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data

COPY main.py .
RUN useradd -u 1000 -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]