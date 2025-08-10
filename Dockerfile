# ===== Location Update Bot - Dockerfile (Python 3.11 slim) =====
FROM python:3.11-slim

# System deps (curl useful for debugging; locales optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create app user
RUN useradd -ms /bin/bash appuser
WORKDIR /app

# Copy requirements first (better layer caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Run as non-root
USER appuser

# Optional HEALTHCHECK: enable only if you expose a status endpoint.
# HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD python -c "import sys; sys.exit(0)"

# Default start
CMD ["python", "main.py"]
