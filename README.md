# diavgeia

A set of Python scripts to fetch, back up, and mirror data from [diavgeia.gov.gr](https://diavgeia.gov.gr).

## Features
- Fetches data from diavgeia.gov.gr using their API
- Uses multiple workers for efficient downloading
- Downloads PDFs associated with the data
- Compressses data into ZIP files
- Logs activities and errors
- Dockerized for easy deployment
- Configurable via environment variables and `.env` files

## Configuration

Configuration is managed using environment variables. You can provide them in a `.env` file or via the environment.

Example `.env` file:
```bash
# Diavgeia
DIAVGEIA_API_USER=...
DIAVGEIA_API_PASSWORD=...

# Script config
LOG_LEVEL=INFO

# Paths
LOG_PATH=/tmp/logs
EXPORT_PATH=/tmp/exports

# Workers
CRAWL_WORKERS=1
DOWNLOAD_WORKERS=5
DOWNLOAD_PDF=True
```

## Usage
1. Clone the repository.
2. Create a `.env` file with your configuration.
3. Run the scripts using Python:
   - Make sure you have python and uv installed
   - `uv sync --locked`
   - `python src/main.py --date-id YYYY-MM-DD`
4. Or with Docker:
   - `docker build -t diavgeia-daily:latest .` 
   - `docker run --env-file=.env diavgeia-daily:latest --date-id YYYY-MM-DD`