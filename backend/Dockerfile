FROM python:3.12-slim

ARG INSTALL_PLAYWRIGHT=0

# Install system deps
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright browsers are optional for local UI/API bootstraps.
# Skip the large browser download unless explicitly requested at build time.
RUN if [ "$INSTALL_PLAYWRIGHT" = "1" ]; then playwright install chromium; fi

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000
