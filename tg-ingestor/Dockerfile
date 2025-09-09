# Multi-stage build for tg-ingestor service
FROM python:3.11-slim as builder

# Set build arguments
ARG POETRY_VERSION=1.6.1

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and use non-root user for building
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set work directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set work directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser . .

# Create directories for data, logs, and session
RUN mkdir -p /app/data /app/logs && \
    chown -R appuser:appuser /app

# Create volume mount point for session data
VOLUME ["/app/data"]

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set Telethon session path to persistent volume
ENV TELEGRAM_SESSION=/app/data/session.session

# Health check using the built-in health check function
HEALTHCHECK --interval=60s --timeout=15s --start-period=30s --retries=3 \
    CMD python app.py healthcheck || exit 1

# Default command
CMD ["python", "app.py"]