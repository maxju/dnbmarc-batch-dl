# Base Image
FROM python:3.11-slim AS base

ENV data_dir=/data
ENV download_dir=/data/files

WORKDIR /app
COPY python .

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     libxml2-dev \
#     libxslt-dev \
#     python3-dev \
#     python3-pip \
#     poppler-utils \
#     && rm -rf /var/lib/apt/lists/*

# Downloader Image
FROM base AS downloader
WORKDIR /app/downloader
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "downloader.py"]

# Metadata Extractor Image
FROM base AS metadata_extractor
WORKDIR /app/metadata_extractor
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "metadata_extractor.py"]

FROM base AS abstract_finder
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app/abstract_finder
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "find_abstract_app.py"]