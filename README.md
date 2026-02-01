# 🎬 IMDB Analytics Platform

A comprehensive data platform built on IMDB datasets, featuring automated data ingestion, dbt transformations, and an LLM-powered chat interface for natural language queries.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.9+-orange.svg)](https://www.getdbt.com/)
[![Streamlit](https://img.shields.io/badge/streamlit-1.39+-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#️-architecture)
- [Features](#-features)
- [Tech Stack](#️-tech-stack)
- [Getting Started](#-getting-started)
- [Project Structure](#-project-structure)
- [Usage](#-usage)
- [Deployment](#-deployment)
- [Data Model](#-data-model)
- [Development](#-development)
- [Acknowledgments](#-acknowledgments)
- [License](#license)

## 🎯 Overview

This project demonstrates a complete modern data platform workflow:

1. **Data Ingestion**: Automated daily ingestion from IMDB datasets to BigQuery
2. **Data Transformation**: SQL-based transformations using dbt for analytical models
3. **LLM Interface**: Natural language query interface powered by OpenAI GPT-4o

The platform enables users to ask questions like:
- "What are the top 10 highest-rated movies?"
- "Show me Christopher Nolan's filmography"
- "Which actors worked with Martin Scorsese?"

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    IMDB Datasets                            │
│              (7 TSV files, updated daily)                   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
              ┌────────────────┐
              │   Ingestion    │
              │   (Python)     │
              └────────┬───────┘
                       │
                       ▼
              ┌────────────────┐
              │   BigQuery     │
              │ Bronze Layer   │
              │  (7 tables)    │
              └────────┬───────┘
                       │
                       ▼
              ┌────────────────┐
              │  dbt Transform │
              │ Silver Layer   │
              │  (8 models)    │
              └────────┬───────┘
                       │
                       ▼
       ┌───────────────┴────────────────┐
       │                                │
       ▼                                ▼
┌──────────────┐              ┌─────────────────┐
│  Streamlit   │              │   Cron Job      │
│  + GPT-4o    │              │  (Daily 2 AM)   │
│ Chat Interface│              │  Auto-ingestion │
└──────────────┘              └─────────────────┘
```

## ✨ Features

### Data Ingestion
- ✅ Automated download from IMDB datasets (7 TSV files)
- ✅ Conversion to Parquet format for efficiency
- ✅ Load into BigQuery bronze layer
- ✅ Idempotent pipeline (safe to re-run)
- ✅ Scheduled daily execution via cron job

### Data Transformation (dbt)
- ✅ 5 staging models for data cleaning
- ✅ 3 marts models (movies, dim_actors, dim_directors)
- ✅ Partitioned and clustered tables for performance
- ✅ Comprehensive data quality tests
- ✅ Full documentation and lineage

### LLM Chat Interface
- ✅ Natural language to SQL conversion via GPT-4o
- ✅ Automatic SQL execution on BigQuery
- ✅ Interactive chat with conversation history
- ✅ Model selection (GPT-4o, GPT-4o-mini, GPT-4-turbo)
- ✅ Usage statistics tracking (tokens, questions, session time)
- ✅ CSV export functionality

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Cloud Platform** | Google Cloud Platform (GCP) |
| **Data Warehouse** | BigQuery |
| **Data Pipeline** | Python 3.11, Pandas, PyArrow |
| **Transformation** | dbt |
| **LLM** | OpenAI GPT-4o |
| **Frontend** | Streamlit |
| **Containerization** | Docker |
| **CI/CD** | GitHub Actions |
| **Package Management** | uv |
| **Code Quality** | Ruff (linter + formatter) |

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Google Cloud Platform account with BigQuery API enabled
- OpenAI API key
- `uv` package manager ([installation](https://github.com/astral-sh/uv))

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Richard-GOZAN/imdb-analytics-platform.git
   cd imdb-analytics-platform
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Configure environment**
   ```bash
   cp .env.template .env
   # Edit .env with your credentials
   ```

4. **Run ingestion**
   ```bash
   uv run python ingestion/ingest.py
   ```

5. **Run dbt transformations**
   ```bash
   cd dbt/transform
   uv run dbt run
   ```

6. **Launch chat app**
   ```bash
   uv run streamlit run app/chat.py
   # Access: http://localhost:8501
   ```

## 📁 Project Structure

```
imdb-analytics-platform/
├── ingestion/              # Data ingestion pipeline
│   ├── config.py          # Configuration
│   ├── ingest.py          # Main script
│   └── utils.py           # Helper functions
├── dbt/transform/         # dbt transformation project
│   ├── models/
│   │   ├── staging/       # Staging models (5)
│   │   └── marts/         # Marts models (3)
│   ├── dbt_project.yml
│   └── profiles.yml
├── app/                   # LLM chat application
│   ├── chat.py           # Streamlit interface
│   ├── agent.py          # OpenAI agent
│   ├── bigquery_tool.py  # BigQuery execution
│   └── stats.py          # Usage tracking
├── docker/               # Docker configurations
│   ├── Dockerfile.app
│   └── Dockerfile.ingestion
├── scripts/              # Automation scripts
│   ├── run_ingestion.sh  # Pipeline wrapper
│   └── setup_cron.sh     # Cron installation
├── .github/workflows/    # CI/CD
│   ├── test.yml         # Tests
│   └── deploy-app.yml   # Deployment
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## 💡 Usage

### Ingestion Pipeline

```bash
# Full ingestion
uv run python ingestion/ingest.py

# Force re-download
uv run python ingestion/ingest.py --force-download
```

**Datasets ingested:**
- `name.basics` - Person information
- `title.basics` - Movie information
- `title.ratings` - Ratings and votes
- `title.crew` - Director assignments
- `title.principals` - Principal cast/crew
- `title.akas` - Alternative titles
- `title.episode` - TV episode info

### dbt Transformations

```bash
cd dbt/transform

# Run all models
uv run dbt run

# Run tests
uv run dbt test

# Generate docs
uv run dbt docs generate && dbt docs serve
```

**Models:**
- **Staging** (5 models): Data cleaning and filtering
- **Marts** (3 models):
  - `movies` - Fact table (11.9K rows)
  - `dim_actors` - Actor dimension (30.7K actors)
  - `dim_directors` - Director dimension (3.6K directors)

### Chat Application

```bash
uv run streamlit run app/chat.py
```

**Example queries:**
- "Top 10 highest-rated movies"
- "Christopher Nolan's filmography"
- "Actors who worked with Scorsese"

## 🚢 Deployment

### Local with Docker

```bash
# Build
docker compose build

# Run app
docker compose up app

# Run ingestion
docker compose run --rm ingestion
```

### Production (GCP VM)

```bash
# Setup cron job
./scripts/setup_cron.sh

# Runs daily at 2 AM automatically
```

### CI/CD

GitHub Actions workflows:
- **Lint**: Automatic linting on push
- **Deploy**: Manual deployment to VM via SSH

## 📊 Data Model

### Movies (Fact Table)
```sql
movie_id          STRING
title             STRING
release_year      INTEGER
average_rating    FLOAT64
num_votes         INTEGER
directors         ARRAY<STRUCT<id, name, birth_year>>
actors            ARRAY<STRUCT<id, name, characters, gender>>
```

### Dimensions
- `dim_directors` - Director details with filmography
- `dim_actors` - Actor details with career stats

## 🧪 Development

### Code Quality

Pre-commit hooks automatically run:
- Code formatting (Ruff)
- Import sorting (Ruff)
- Style checks (Ruff)

```bash
# Run linting
uv run ruff check .

# Run formatting
uv run ruff format .
```


## 🙏 Acknowledgments

- [IMDB Datasets](https://developer.imdb.com/non-commercial-datasets/)
- [dbt Labs](https://www.getdbt.com/)
- [OpenAI](https://openai.com/)

## 📝 License

MIT License - see [LICENSE](LICENSE)

---

⭐ If you find this project useful, please star it!
