import os
import logging
from google.cloud import bigquery

# -------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------------------------------------
# AUTHENTICATION
# -------------------------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"D:\python try hard\unigap\project6\data\json_key\fresh-ocean-475916-m2-d87215690697.json"
)

# -------------------------------------------------
# GENERIC LOAD FUNCTION
# -------------------------------------------------
def load_jsonl_to_bigquery(
    project_id,
    dataset_id,
    table_id,
    gcs_uri,
    schema,
    write_mode=bigquery.WriteDisposition.WRITE_TRUNCATE,  # DEFAULT = FULL RELOAD
):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,                # Explicit schema
        write_disposition=write_mode  # TRUNCATE or APPEND
    )

    logging.info("--------------------------------------------------")
    logging.info("START LOAD JOB")
    logging.info(f"Table: {table_ref}")
    logging.info(f"Source: {gcs_uri}")
    logging.info(f"Write mode: {write_mode}")
    logging.info("--------------------------------------------------")

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()  # Wait until finished

    table = client.get_table(table_ref)
    logging.info(f"SUCCESS | Rows in table: {table.num_rows}")
    logging.info("--------------------------------------------------")


# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PROJECT_ID = "fresh-ocean-475916-m2"
DATASET_ID = "glamira_dataset"

# -------------------------------------------------
# SCHEMAS
# -------------------------------------------------
ip_location_schema = [
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("city", "STRING"),
]

product_ids_to_crawl_schema = [
    bigquery.SchemaField("product_id", "INTEGER"),
    bigquery.SchemaField("url", "STRING"),
]

crawl_product_ids_schema = [
    bigquery.SchemaField("product_id", "INTEGER"),
]

summary_schema = [
    bigquery.SchemaField("_id", "STRING"),  
    bigquery.SchemaField("api_version", "STRING"),
    bigquery.SchemaField("collection", "STRING"),
    bigquery.SchemaField("device_id", "STRING"),
    bigquery.SchemaField("email_address", "STRING"),
    bigquery.SchemaField("ip", "STRING"),
    bigquery.SchemaField("local_time", "STRING"),
    bigquery.SchemaField("resolution", "STRING"),
    bigquery.SchemaField("current_url", "STRING"),
    bigquery.SchemaField("referrer_url", "STRING"),
    bigquery.SchemaField("show_recommendation", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),
    bigquery.SchemaField("time_stamp", "INTEGER"),
    bigquery.SchemaField("user_agent", "STRING"),
    bigquery.SchemaField("user_id_db", "STRING"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("price", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("recommendation", "BOOLEAN"),
    bigquery.SchemaField("recommendation_clicked_position", "INTEGER"),
    bigquery.SchemaField("recommendation_product_id", "STRING"),
    bigquery.SchemaField("utm_medium", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("viewing_product_id", "STRING"),
    bigquery.SchemaField("cat_id", "STRING"),
    bigquery.SchemaField("collect_id", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("is_paypal", "BOOLEAN"),
    bigquery.SchemaField("key_search", "STRING"),

    bigquery.SchemaField(
        "cart_products",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("amount", "INTEGER"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField(
                "option",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("option_id", "INTEGER"),
                    bigquery.SchemaField("option_label", "STRING"),
                    bigquery.SchemaField("value_id", "INTEGER"),
                    bigquery.SchemaField("value_label", "STRING"),
                    bigquery.SchemaField("raw", "STRING"),
                ],
            ),
        ],
    ),

    bigquery.SchemaField(
        "option",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("Kollektion", "STRING"),
            bigquery.SchemaField("alloy", "STRING"),
            bigquery.SchemaField("category_id", "STRING"),
            bigquery.SchemaField("diamond", "STRING"),
            bigquery.SchemaField("finish", "STRING"),
            bigquery.SchemaField("kollektion_id", "STRING"),
            bigquery.SchemaField("option_id", "STRING"),
            bigquery.SchemaField("option_label", "STRING"),
            bigquery.SchemaField("pearlcolor", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("quality", "STRING"),
            bigquery.SchemaField("quality_label", "STRING"),
            bigquery.SchemaField("shapediamond", "STRING"),
            bigquery.SchemaField("stone", "STRING"),
            bigquery.SchemaField("value_id", "STRING"),
            bigquery.SchemaField("value_label", "STRING"),
        ],
    ),
]

# -------------------------------------------------
# LOAD JOBS
# -------------------------------------------------

# FULL RELOAD TABLES (ROW COUNT STAYS CONSTANT)
load_jsonl_to_bigquery(
    PROJECT_ID,
    DATASET_ID,
    "ip_locations",
    "gs://twan_glamira/dataset_export/ip_location_results.jsonl",
    ip_location_schema
)

load_jsonl_to_bigquery(
    PROJECT_ID,
    DATASET_ID,
    "product_ids_to_crawl",
    "gs://twan_glamira/dataset_export/product_ids_to_crawl.jsonl",
    product_ids_to_crawl_schema
)

load_jsonl_to_bigquery(
    PROJECT_ID,
    DATASET_ID,
    "crawl_product_id",
    "gs://twan_glamira/dataset_export/valid_product_ids.jsonl",
    crawl_product_ids_schema
)

# APPEND-ONLY TABLE
load_jsonl_to_bigquery(
    PROJECT_ID,
    DATASET_ID,
    "glamira_raw",
    "gs://twan_glamira/dataset_export/summary/summary_*.jsonl",
    summary_schema,
    bigquery.WriteDisposition.WRITE_APPEND
)


