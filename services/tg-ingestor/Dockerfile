# Multi-stage build для tg-ingestor
FROM python:3.11-slim as builder

# Системные зависимости
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Копируем и устанавливаем shared library
COPY libs/queue-client/ ./libs/queue-client/
RUN pip install --no-cache-dir ./libs/queue-client/

# Копируем и устанавливаем зависимости сервиса  
COPY services/tg-ingestor/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim

# Создаем пользователя
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Копируем установленные пакеты из builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Рабочая директория
WORKDIR /app

# Копируем код сервиса
COPY services/tg-ingestor/ .

# Создаем директории и права
RUN mkdir -p /app/data /app/logs && \
    chown -R appuser:appuser /app

USER appuser

# Environment
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=60s --timeout=15s --start-period=30s --retries=3 \
    CMD python app.py healthcheck || exit 1

CMD ["python", "app.py"]