import logging
import os
from flask import Request, jsonify
from google.cloud import bigquery

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PROJECT_ID = "fresh-ocean-475916-m2"
DATASET_ID = "glamira_dataset"

TABLE_MAP = {
    "ip_location_results": "ip_locations",
    "product_ids_to_crawl": "product_ids_to_crawl",
    "valid_product_ids": "crawl_product_id",
    "summary": "glamira_raw"
}

logging.basicConfig(level=logging.INFO)

# -------------------------------------------------
# CLOUD RUN ENTRYPOINT (HTTP)
# -------------------------------------------------
def trigger_bigquery_load(request: Request):
    """
    HTTP Cloud Run service.
    Triggered by GCS finalized event or manual test.
    """

    try:
        event = request.get_json(silent=True)
        if not event:
            return ("No JSON body received", 400)

        file_name = event.get("name")
        bucket = event.get("bucket")

        if not file_name or not bucket:
            return ("Missing name or bucket", 400)

        logging.info(f"Received file: {file_name}")
        logging.info(f"Bucket: {bucket}")

        # -----------------------------
        # Detect table from filename
        # -----------------------------
        base = os.path.basename(file_name).lower()

        table_name = None
        for prefix, table in TABLE_MAP.items():
            if base.startswith(prefix):
                table_name = table
                break

        if not table_name:
            logging.info("File not mapped to any table. Skipping.")
            return ("Ignored", 200)

        # -----------------------------
        # BigQuery setup
        # -----------------------------
        client = bigquery.Client(project=PROJECT_ID)

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        uri = f"gs://{bucket}/{file_name}"

        # 🔥 SINGLE SOURCE OF TRUTH: FETCH TABLE SCHEMA
        table = client.get_table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=table.schema,  # ✅ reuse existing schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True
        )

        logging.info(f"Loading {uri} → {table_id}")

        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        load_job.result()

        logging.info("Load completed successfully")

        return jsonify({
            "status": "success",
            "table": table_name,
            "rows_loaded": load_job.output_rows
        })

    except Exception as e:
        logging.exception("BigQuery load failed")
        return (str(e), 500)
