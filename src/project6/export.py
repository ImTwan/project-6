import csv
import json
import os
import logging
from google.oauth2 import service_account
from google.cloud import storage
from pymongo import MongoClient
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor

# -----------------------------------------------------------
# Helper functions
# -----------------------------------------------------------
def convert_oid(val):
    """Convert MongoDB ObjectId to string."""
    if isinstance(val, ObjectId):
        return str(val)
    return val

def cast_value(val, field_type):
    """Cast value to appropriate type for BigQuery."""
    if val is None:
        return None
    try:
        if field_type == "STRING":
            return str(val)
        elif field_type == "INTEGER":
            return int(val)
        elif field_type == "BOOLEAN":
            return bool(val)
        elif field_type == "FLOAT":
            return float(val)
        else:
            return val
    except Exception:
        return None

def normalize_doc(doc, schema):
    """Normalize a MongoDB document according to a given schema."""
    out = {}
    for field, field_schema in schema.items():

        # Primitive field
        if isinstance(field_schema, str):
            val = doc.get(field)
            val = convert_oid(val)
            
            # ✅ FORCE api_version to STRING ALWAYS
            if field == "api_version" and val is not None:
                out[field] = str(val)
            else:
                out[field] = cast_value(val, field_schema)

            continue

        # RECORD field
        if isinstance(field_schema, dict):
            if field_schema.get("type") == "REPEATED":
                arr = doc.get(field, [])
                if not isinstance(arr, list):
                    arr = [arr] if arr else []
                out[field] = []
                for item in arr:
                    if isinstance(item, dict):
                        out[field].append(normalize_doc(item, field_schema["fields"]))
                continue

            # Normal RECORD
            sub = doc.get(field, {})
            if not isinstance(sub, dict):
                out[field] = None
            else:
                out[field] = normalize_doc(sub, field_schema["fields"])

    return out

def upload_file(blob, path):
    """Upload a local file to GCS."""
    blob.upload_from_filename(path, timeout=1800)

# -----------------------------------------------------------
# Summary schema for normalization
# -----------------------------------------------------------
summary_schema = {
    "_id": "STRING",
    "api_version": "STRING",
    "collection": "STRING",
    "device_id": "STRING",
    "email_address": "STRING",
    "ip": "STRING",
    "local_time": "STRING",
    "resolution": "STRING",
    "current_url": "STRING",
    "referrer_url": "STRING",
    "show_recommendation": "STRING",
    "store_id": "STRING",
    "time_stamp": "INTEGER",
    "user_agent": "STRING",
    "user_id_db": "STRING",
    "order_id": "STRING",
    "price": "STRING",
    "product_id": "STRING",
    "recommendation": "BOOLEAN",
    "recommendation_clicked_position": "INTEGER",
    "recommendation_product_id": "STRING",
    "utm_medium": "STRING",
    "utm_source": "STRING",
    "viewing_product_id": "STRING",
    "cat_id": "STRING",
    "collect_id": "STRING",
    "currency": "STRING",
    "is_paypal": "BOOLEAN",
    "key_search": "STRING",
    "cart_products": {
        "type": "REPEATED",
        "fields": {
            "amount": "INTEGER",
            "currency": "STRING",
            "price": "STRING",
            "product_id": "INTEGER",
            "option": {
                "type": "REPEATED",
                "fields": {
                    "option_id": "INTEGER",
                    "option_label": "STRING",
                    "value_id": "INTEGER",
                    "value_label": "STRING",
                    "raw": "STRING",
                }
            }
        }
    },
    "option": {
        "type": "REPEATED",
        "fields": {
            "Kollektion": "STRING",
            "alloy": "STRING",
            "category_id": "STRING",
            "diamond": "STRING",
            "finish": "STRING",
            "kollektion_id": "STRING",
            "option_id": "STRING",
            "option_label": "STRING",
            "pearlcolor": "STRING",
            "price": "STRING",
            "quality": "STRING",
            "quality_label": "STRING",
            "shapediamond": "STRING",
            "stone": "STRING",
            "value_id": "STRING",
            "value_label": "STRING",
        }
    }
}

# -----------------------------------------------------------
# Main export function
# -----------------------------------------------------------
def export_to_gcs():
    LOG_DIR = r"D:\python try hard\unigap\project6\data\log"
    os.makedirs(LOG_DIR, exist_ok=True)  # ensure folder exists
    LOG_FILE = os.path.join(LOG_DIR, "export_to_gcs.log")

    logging.basicConfig(
            filename=LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("START export_to_gcs")

    try:
        # MongoDB connection
        MONGO_URL = "mongodb://localhost:27017/"
        DB_NAME = "countly"
        SUMMARY_COLLECTION = "summary"

        logging.info("Connecting to MongoDB...")
        client = MongoClient(MONGO_URL)
        db = client[DB_NAME]
        summary_col = db[SUMMARY_COLLECTION]
        logging.info("Connected to MongoDB successfully")

        # GCS authentication
        KEY_PATH = r"D:\python try hard\unigap\project6\data\json_key\fresh-ocean-475916-m2-d87215690697.json"
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        storage_client = storage.Client(project="fresh-ocean-475916-m2", credentials=credentials)
        BUCKET_NAME = "twan_glamira"
        bucket = storage_client.bucket(BUCKET_NAME)
        logging.info("GCS authentication successful")

        # Temp folders
        TEMP_DIR = r"D:\python try hard\unigap\project6\data\tmp"
        SUMMARY_TMP_DIR = os.path.join(TEMP_DIR, "summary_export")
        os.makedirs(SUMMARY_TMP_DIR, exist_ok=True)

        # Export summary collection
        logging.info("START exporting MongoDB summary collection")
        MONGO_BATCH = 10000
        FILE_BATCH = 1000000
        MAX_WORKERS = 8

        futures = []
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        file_index = 0
        file_docs = 0
        total_docs = 0
        f_out = None
        current_filename = None
        current_local_path = None
        current_blob_path = None

        cursor = summary_col.find({}, no_cursor_timeout=True).batch_size(MONGO_BATCH)
        try:
            for doc in cursor:
                normalized_doc = normalize_doc(doc, summary_schema)

                # Open new file if needed
                if f_out is None or file_docs >= FILE_BATCH:
                    if f_out:
                        f_out.close()
                        futures.append(executor.submit(upload_file, bucket.blob(current_blob_path), current_local_path))
                        logging.info(f"Submitted {current_blob_path} | docs: {total_docs}")

                    current_filename = f"summary_{file_index:05d}.jsonl"
                    current_local_path = os.path.join(SUMMARY_TMP_DIR, current_filename)
                    current_blob_path = f"dataset_export/summary/{current_filename}"
                    f_out = open(current_local_path, "w", encoding="utf-8")
                    file_index += 1
                    file_docs = 0

                f_out.write(json.dumps(normalized_doc) + "\n")
                file_docs += 1
                total_docs += 1

                if total_docs % 100000 == 0:
                    logging.info(f"Exported {total_docs} documents so far...")

            # Final file
            if f_out:
                f_out.close()
                futures.append(executor.submit(upload_file, bucket.blob(current_blob_path), current_local_path))
                logging.info(f"Submitted FINAL {current_blob_path} | docs: {total_docs}")

        finally:
            cursor.close()

        # Wait for all uploads to finish
        for f in futures:
            f.result()

        executor.shutdown(wait=True)
        logging.info(f"MongoDB export complete. Total docs: {total_docs}")
        print("SUCCESS: Export completed.")

    except Exception as e:
        logging.exception("ERROR in export_to_gcs")
        print("FAILED. Check export_to_gcs.log.")

# -----------------------------------------------------------
# Run export
# -----------------------------------------------------------
if __name__ == "__main__":
    export_to_gcs()
