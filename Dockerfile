FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Broad runtime/toolchain image for universal deployments. On a normal VPS,
# XENORA still uses whatever runtimes the host already provides.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl git build-essential pkg-config \
       nodejs npm php-cli ruby-full ruby-dev \
       golang-go default-jre-headless maven \
       rustc cargo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Persistent storage is REQUIRED in production. Mount /app/data,
# /app/hosted_bots, /app/hosted_websites, /app/pending and /app/logs.
VOLUME ["/app/data", "/app/hosted_bots", "/app/hosted_websites", "/app/pending", "/app/logs"]

CMD ["python", "main.py"]
