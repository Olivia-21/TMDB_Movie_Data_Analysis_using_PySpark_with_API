# TMDB Movie Pipeline

A PySpark-based ETL pipeline for analyzing movie data from The Movie Database (TMDB) API.

## Overview

This pipeline fetches movie data from the TMDB API, cleans and transforms it, computes key performance indicators (KPIs), and generates visualizations for analysis.

## Features

- **Data Extraction**: Fetches movie details and credits from TMDB API with rate limiting
- **Data Cleaning**: Handles JSON parsing, missing values, and data type conversions
- **Data Enrichment**: Calculates profit, ROI, and derived metrics
- **Analysis**: Computes rankings, filters, franchise/director performance
- **Visualization**: Generates 5 types of charts using Matplotlib
- **Dockerized**: Runs in a containerized environment

## Project Structure

```
tmdb_movie_pipeline/
|-- orchestrator/
|   |-- tmdb_pipeline.py        # Main entry point (CLI)
|   |-- pipeline_notebook.ipynb # Interactive notebook version
|
|-- src/
|   |-- extract/
|   |   |-- fetch_movies.py     # API extraction from TMDB
|   |-- transform/
|   |   |-- clean_movies.py     # Data cleaning and preprocessing
|   |   |-- derived_metrics.py  # Derived metrics (ROI, profit, etc.)
|   |-- analysis/
|   |   |-- kpi_rankings.py     # Best/worst movie rankings
|   |   |-- advanced_filters.py # Complex search queries
|   |   |-- franchise_analysis.py
|   |   |-- director_analysis.py
|   |-- visualization/
|   |   |-- plots.py            # Matplotlib visualizations
|   |-- utils/
|       |-- api_client.py       # TMDB API client with rate limiting
|
|-- config/
|   |-- __init__.py             # Package marker
|   |-- settings.yaml           # Configuration file (API keys, paths)
|   |-- logger/
|   |   |-- __init__.py         # Package marker
|   |   |-- logger.py           # Logging configuration
|   |-- retries/
|       |-- __init__.py         # Package marker
|       |-- retry.py            # Retry logic with backoff
|
|-- data/
|   |-- raw/                    # Raw extracted data
|   |   |-- movies_raw.csv
|   |-- processed/              # Cleaned data
|   |   |-- movies_cleaned.csv
|   |-- analytics/              # Final enriched data and plots
|       |-- movies_final.csv
|       |-- plots/              # Generated visualizations
|
|-- logs/                       # Pipeline execution logs
|   |-- pipeline.log            # Main orchestration logs
|   |-- extract.log             # Extraction step logs
|   |-- transform.log           # Transformation logs
|   |-- analysis.log            # Analysis step logs
|   |-- visualization.log       # Visualization logs
|
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
|-- README.md
```

## Requirements

- Python 3.9+
- Java 8/11/17 (for PySpark)
- Docker (for containerized execution)

## Quick Start

### Using Docker 

1. Build the Docker image:
   ```bash
   docker-compose build
   ```

2. Run the pipeline:
   ```bash
   docker-compose up
   ```

3. Check the output:
   - Data files: `data/raw/`, `data/processed/`, `data/analytics/`
   - Visualizations: `data/analytics/plots/`
   - Logs: `logs/pipeline.log`

### Local Execution

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure Java is installed and JAVA_HOME is set

3. Run the pipeline:
   ```bash
   python orchestrator/tmdb_pipeline.py
   ```

## Configuration

Edit `config/settings.yaml` to customize:

- **API settings**: API key, timeout, rate limiting
- **File paths**: Input/output locations
- **Movie IDs**: List of movies to analyze
- **Spark settings**: App name, master URL

## KPIs Computed

1. **Revenue Rankings**: Highest/lowest revenue movies
2. **Budget Analysis**: Highest budget movies
3. **Profitability**: Highest/lowest profit movies
4. **ROI Analysis**: Best/worst return on investment (budget >= 10M)
5. **Popularity**: Most popular movies by TMDB score
6. **Ratings**: Highest/lowest rated (with >= 10 votes)

## Visualizations Generated

1. Revenue vs Budget scatter plot
2. ROI by Genre bar chart
3. Popularity vs Rating scatter plot
4. Yearly Box Office trends (bar + line)
5. Franchise vs Standalone comparison

## Logging

Logs are written to:
- `logs/pipeline.log` - Main pipeline logs
- `logs/extract.log` - Extraction step logs
- `logs/transform.log` - Transformation logs

## Error Handling

The pipeline includes:
- Retry logic with exponential backoff for API calls
- Graceful handling of missing or invalid data
- Comprehensive error logging
