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
├── .python-version
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
```
 uv run src/project6/export.py
```
* Load GCS → BigQuery manually
```
 uv run src/project6/load_data.py
```
* Trigger BigQuery from local test
```
 uv run src/project6/trigger_bigquery_test_on_local.py
```

## 4. Project Overview
This project is a continuation of Project 5: Data Collection, Storage, and Foundation (link to project: https://github.com/ImTwan/Project-05-Data-Collection-Storage-Foundation?tab=readme-ov-file#data-collection-storage-foundation). The deliverables of this project are:
* Automated data pipeline
* Cloud Function triggers
* BigQuery tables
* Monitoring setup
Below are the steps for this project 
### 4.1. Data Export Process
#### 4.1.1. Create Python script to: export.py file <br>
#### 4.1.2. GCS Setup (To connect Python from local machine to your GCS bucket): 
* Go to IAM & Admin → Service Accounts
* Create new service account
* Give role: Owner (for learning, downgrade later)
* Generate JSON key (3-dot menu → Manage Keys → Add Key → JSON)
* Save .json file

### 4.2. BigQuery Integration
#### 4.2.1. Create BigQuery dataset named glamira_dataset <br>
#### 4.2.2. Create tables in the dataset: ip_locations, product_ids_to_crawl, crawl_product_id, <br>
#### 4.2.3. Write script to load data from GCS to raw layer in Bigquery (load_data.py)<br>
#### 4.2.4. Set up automated triggers using Cloud Functions: 
* Set the region to the same region as your GCS bucket. For example, the bucket's region is us-central1, then the Cloud Run Function must be us-central1
* Save and deploy the trigger_bigquery_test_on_GCP.py script
* In the Triggers tab, choose Cloud Storage as Event Provider, google.cloud.storage.object.v1.finalized as Event type, Receive events from the GCS bucket you use from the project 5
