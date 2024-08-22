# MongoDB to BigQuery Pipeline
## Setup

### Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)  
- [Docker](https://www.docker.com/products/docker-desktop/)
- [gcloud CLI](https://cloud.google.com/sdk/gcloud)
- `credentials.json` file in the root directory for Google Cloud credentials
- `.env` file in the root directory for environment variables

### Getting Started

1. Clone the repository

```bash
git clone https://github.com/godalida/bookeepee.git
cd mongodb-pipelines
```

2. Ensure the presence of `credentials.json` and `.env` files in the root directory.

3. Install the dependencies

```bash
poetry install
```

4. Activate the poetry shell

```bash
poetry shell
```

5. Running the pipeline

```bash
cd pipelines
python mongodb_pipeline.py
```