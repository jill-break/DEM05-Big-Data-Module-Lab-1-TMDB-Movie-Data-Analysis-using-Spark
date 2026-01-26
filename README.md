# TMDB Movie Data Analysis (PySpark)

A compact ETL and analysis project that ingests movie data from The Movie Database (TMDB) API, processes and analyzes it with PySpark, computes KPIs (ROI, profitability, franchise stats), and generates visualizations.

---

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data](#data)
- [Notebooks & Scripts](#notebooks--scripts)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This repository demonstrates a reproducible data pipeline for movie analytics using the TMDB API. The pipeline shows ingestion, schema-enforced transformation with Spark, KPI computation, and visualization for exploratory analysis.

## Requirements

- Python 3.11
- Java 17 (for Spark)
- A TMDB API key (register at https://www.themoviedb.org)
- Recommended: virtual environment (venv)


## Project Structure

```
README.md
requirements.txt
data/
	├─ raw/
	└─ processed/
notebooks/
	└─ spark_lab.ipynb
src/
	├─ __init__.py
	├─ config.py         # environment & schemas
	├─ utils.py          # spark session factory & helpers
	├─ ingestion.py      # TMDB API fetchers
	├─ cleaning.py       # PySpark cleaning & transformations
	├─ analysis.py       # KPI computations
	└─ visualization.py  # plotting helpers
tests/
	├─ __init__.py
	└─ test_cleaning.py 
```

## Data

- `data/raw/` — raw JSON responses from TMDB (not committed to git)
- `data/processed/` — cleaned parquet/csv results used by analyses and visualizations

## Notebooks & Scripts

- `notebooks/spark_lab.ipynb` — main notebook orchestrating the workflow.
- Use functions in `src/` to run parts of the pipeline 

## License

Educational use — data provided by The Movie Database (TMDB). Check TMDB terms for API usage restrictions.


