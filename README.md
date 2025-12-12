# Project 06: Data Pipeline & Storage
## 1. Project Structure
<pre>
project-6/
├── logs/
├── src/
│   └── project6/
│       ├── export.py
│       ├── load_data.py
│       ├── trigger_bigquery_test_on_GCP.py
│       └── trigger_bigquery_test_on_local.py
├── .gitignore
├── .python*version
├── README.md
├── pyproject.toml
└── uv.lock
</pre>
## 2. Installation & Environment Setup
* Install uv:
```text
 pip install uv
```
* Install dependencies:
```text
 uv sync
```

## 3. Usage
From project root:
* Export MongoDB → GCS
```text
 uv run src/project6/export.py
```
* Load GCS → BigQuery manually
```text
 uv run src/project6/load_data.py
```
* Trigger BigQuery from local test
```text
 uv run src/project6/trigger_bigquery_test_on_local.py
```

