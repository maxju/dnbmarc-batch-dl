# Base Image
FROM python:3.11-slim AS base

ENV data_dir=/data
ENV download_dir=/data/files

WORKDIR /app
COPY python .

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

# Downloader Image
FROM base AS downloader
CMD ["python", "downloader.py"]

# Metadata Extractor Image
FROM base AS metadata_extractor
CMD ["python", "metadata_extractor.py"]