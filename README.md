# DNBMarc Batch Download and Processing

A collection of tools for analyzing German Open Access theses from the German National Library (DNB), including metadata extraction, metadata enrichment, PDF processing, and abstract finding.
Base for the process is a dnbmarc-xml file containing the metadata and access urls of the documents.
Pipeline is XML -> SQL -> PDFs ->  Marker Markdown (.mmd)
## Components

### 1. Metadata Extraction
- `python/metadata_extractor/`: Extracts metadata from MARC XML records
- Processes fields like title, author, DDC classification, URLs, etc.
- Stores data in PostgreSQL database

### 2. PDF Processing
- `python/downloader/`: Downloads PDFs from DNB archive URLs
- `python/converter/`: Converts PDFs to markdown format
- `python/abstract_finder/`: Looks for the existence and place of the string "abstract" in PDFs

### 3. Analysis Tools
Located in `notebooks/`

## Setup

1. Setup Env files
2. (for services) Build docker images for the components you want to use. For build targets look at `Dockerfile`

## Usage

- For services: Either run Docker Images using `docker run` or `docker-compose.yml` or install `requirements.txt` in your environment and run the scripts directly.
- For ipynb-scripts: `pip install -r requirements`and run.

## Data Structure

The main database table (`dnb_records`) contains:
- Basic metadata (title, author, year)
- Classification (DDC)
- File information (path, size, pages)
- Processing status (converted, abstract found)
- GND identifiers
- more metadate you cat get from `pg_model.py`

## Analysis Notebooks

Use the Jupyter notebooks in `notebooks/` for:
- Statistical analysis of the dataset
- Abstract position patterns
- Token count distribution
- DDC classification analysis
- GND connections
- Generating subsets with certain features or ddc distribution

