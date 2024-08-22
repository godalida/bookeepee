# MongoDB to BigQuery Pipeline
## Setup

### Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)  
- [Docker](https://www.docker.com/products/docker-desktop/)
- [gcloud CLI](https://cloud.google.com/sdk/gcloud)

### Getting Started

1. Clone the repository

```bash
git clone https://github.com/godalida/bookeepee.git
cd mongodb-pipelines
```

2. Install the dependencies

```bash
poetry install
```

3. Activate the poetry shell

```bash
poetry shell
```

4. Running the pipeline

```bash
cd pipelines
python mongodb_pipeline.py
```