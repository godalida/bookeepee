[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Product Pipelines Tests](https://github.com/TriallyAI/elt-ingest-pipelines/actions/workflows/build-and-deploy-product-pipelines.yaml/badge.svg)](https://github.com/TriallyAI/elt-ingest-pipelines/actions/workflows/build-and-deploy-product-pipelines.yaml)

# elt-ingest-pipelines

This repo contains the code used to ingest data from external sources into our data lake. The project uses [dlt](https://github.com/dlt-hub/dlt) to make the data loading tasks easier.

## Setup

### Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)  
- [Docker](https://www.docker.com/products/docker-desktop/)
- [gcloud CLI](https://cloud.google.com/sdk/gcloud)

### Getting Started

1. Clone the repository

```bash
git clone https://github.com/TriallyAI/elt-ingest-pipelines.git
cd elt-ingest-pipelines
```

2. Install the dependencies

```bash
poetry install
```

3. Activate the poetry shell

```bash
poetry shell
```