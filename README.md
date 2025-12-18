# Project 06: Data Pipeline & Storage
## 1. Project Structure
<pre>
project-6/
├── logs/
├── src/
│   └── project6/
│       ├── export_csv_files.py # Export CSV files to JSONL then upload to GCS bucket 
│       ├── export.py # Export summary collection from MongoDB to JSONL then upload to GCS bucket 
│       ├── load_data.py # load data from GCS bucket to BigQuery tables
│       └── trigger_bigquery_test_on_GCP.py
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
* Export summary collection from MongoDB to JSONL then upload to GCS bucket 
```
 uv run src/project6/export.py
```
* Export CSV files to JSONL then upload to GCS bucket
```
 uv run src/project6/export_csv_files.py
```
* Load GCS → BigQuery manually
```
 uv run src/project6/load_data.py
```
* Trigger BigQuery from using cloud run function 
```
 uv run src/project6/trigger_bigquery_test_on_GCP.py
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
* Save and deploy the code in main.py file using the trigger_bigquery_test_on_GCP.py code
* Save and deploy these libraries for the requirement.txt file: 
```
functions-framework
flask
google-cloud-bigquery
google-cloud-storage
```
* In the Triggers tab, choose Cloud Storage as Event Provider, google.cloud.storage.object.v1.finalized as Event type, Receive events from the GCS bucket you use from the project 5
* Click the Test button to test the cloud run function, then run the script on to test on Cloud Shell:
```
curl -X POST "https://prj6-1013748103239.us-central1.run.app" \
> -H "Authorization: bearer $(gcloud auth print-identity-token)" \
> -H "Content-Type: application/json" \
> -H "ce-id: 1234567890" \
> -H "ce-specversion: 1.0" \
> -H "ce-type: google.cloud.storage.object.v1.finalized" \
> -H "ce-time: 2020-08-08T00:11:44.895529672Z" \
> -H "ce-source: //storage.googleapis.com/projects/_/buckets/twan_glamira" \
> -d '{  "name": "THE FILES on GCS BUCKET YOU WANT TO TEST(For example: dataset_export/ip_location_results.jsonl)",  "bucket": "YOUR BUCKET'S NAME ON GCS BUCKET",  "contentType": "application/json",  "metageneration": "1",  "timeCreated": "2020-04-23T07:38:57.230Z",  "updated": "2020-04-23T07:38:57.230Z" }'

```
