import os
import logging
from flask import Flask, request
from google.cloud import bigquery

# --------------------------------
# App setup (REQUIRED for Cloud Run)
# --------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# --------------------------------
# Config
# --------------------------------
PROJECT_ID = "fresh-ocean-475916-m2"
DATASET_ID = "glamira_dataset"

TABLE_MAP = {
    "ip_location_results": "ip_locations",
    "product_ids_to_crawl": "product_ids_to_crawl",
    "valid_product_ids": "crawl_product_id",
    "summary": "glamira_raw"
}

# --------------------------------
# HTTP endpoint
# --------------------------------
@app.route("/", methods=["POST"])
def trigger_bigquery_load():
    try:
        data = request.get_json(silent=True)
        if not data:
            return ("Bad Request: No JSON body", 400)

        file_name = data.get("name")
        bucket_name = data.get("bucket")

        if not file_name or not bucket_name:
            return ("Bad Request: Missing file or bucket", 400)

        logging.info(f"Triggered by file: {file_name}")

        filename_only = os.path.basename(file_name)
        prefix = filename_only.split(".")[0]

        # Handle summary files: summary_00001.jsonl
        if prefix.startswith("summary_"):
            table_name = "glamira_raw"
        else:
            table_name = TABLE_MAP.get(prefix)

        if not table_name:
            logging.info("File not mapped to BigQuery table. Skipping.")
            return ("Ignored", 200)

        uri = f"gs://{bucket_name}/{file_name}"
        client = bigquery.Client(project=PROJECT_ID)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True
        )

        load_job = client.load_table_from_uri(
            uri,
            f"{PROJECT_ID}.{DATASET_ID}.{table_name}",
            job_config=job_config
        )

        load_job.result()

        logging.info(f"Loaded {uri} into {table_name}")
        return ("OK", 200)

    except Exception as e:
        logging.exception("Failed")
        return (str(e), 500)


# --------------------------------
# REQUIRED for Cloud Run
# --------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
