import csv
import json
import os
import logging
from google.oauth2 import service_account
from google.cloud import storage
from pymongo import MongoClient
from datetime import datetime


def export_to_gcs():

    # -----------------------------------------------------------
    # Logging setup
    # -----------------------------------------------------------
    LOG_DIR = r"D:\python try hard\unigap\project6\data\log"
    if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)

    LOG_FILE = os.path.join(LOG_DIR, "export_to_gcs.log")

    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("START export_to_gcs")

    try:
        # -----------------------------------------------------------
        # 1. Connect to MongoDB VM
        # -----------------------------------------------------------
        MONGO_URL = "mongodb://34.134.190.155:27017/"
        DB_NAME = "countly"

        logging.info("Connecting to MongoDB...")
        client = MongoClient(MONGO_URL)
        db = client[DB_NAME]
        logging.info("Connected to MongoDB successfully")

        # -----------------------------------------------------------
        # 2. Load service account credentials
        # -----------------------------------------------------------
        KEY_PATH = r"D:\python try hard\unigap\project6\data\json_key\fresh-ocean-475916-m2-d87215690697.json"

        logging.info("Loading GCS credentials...")

        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

        storage_client = storage.Client(
            project="fresh-ocean-475916-m2",
            credentials=credentials
        )

        logging.info("GCS authentication successful")

        # -----------------------------------------------------------
        # 3. Files to export
        # -----------------------------------------------------------
        csv_files = [
            r"D:\python try hard\unigap\project6\data\csv\ip_location_results.csv",
            r"D:\python try hard\unigap\project6\data\csv\product_ids_to_crawl.csv",
            r"D:\python try hard\unigap\project6\data\csv\valid_product_ids.csv"
        ]

        # BUCKET_NAME = "project5a"
        BUCKET_NAME = "twan_glamira"

        # Temp directory for JSONL files
        TEMP_DIR = r"D:\python try hard\unigap\project6\data\tmp"

        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)
            logging.info(f"Created TEMP directory: {TEMP_DIR}")

        # -----------------------------------------------------------
        # Process each CSV
        # -----------------------------------------------------------
        for csv_file in csv_files:
            logging.info(f"Processing file: {csv_file}")

            base_name = os.path.splitext(os.path.basename(csv_file))[0]
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # jsonl_file = os.path.join(TEMP_DIR, f"{base_name}_{timestamp}.jsonl")

            jsonl_file = os.path.join(TEMP_DIR, f"{base_name}.jsonl")

            logging.info(f"Converting {csv_file} to {jsonl_file}")

            # Convert CSV → JSONL
            with open(csv_file, "r", encoding="utf-8") as f_in, \
                 open(jsonl_file, "w", encoding="utf-8") as f_out:

                reader = csv.reader(f_in)
                headers = next(reader)

                batch = []
                batch_size = 50000
                total = 0

                for row in reader:
                    doc = {headers[i]: row[i] for i in range(len(headers))}
                    batch.append(doc)
                    total += 1

                    if len(batch) >= batch_size:
                        for item in batch:
                            f_out.write(json.dumps(item) + "\n")
                        batch.clear()

                # Write final batch
                for item in batch:
                    f_out.write(json.dumps(item) + "\n")

            logging.info(f"CSV → JSONL complete. Rows: {total}")

            # -----------------------------------------------------------
            # Upload to GCS
            # -----------------------------------------------------------
            bucket = storage_client.bucket(BUCKET_NAME)

            # blob_path = f"project6_export/{base_name}_{timestamp}.jsonl"

            blob_path = f"dataset_export/{base_name}.jsonl"
            blob = bucket.blob(blob_path)

            logging.info(f"Uploading to gs://{BUCKET_NAME}/{blob_path}")

            # IMPORTANT: Force passing the client for credentials
            blob.upload_from_filename(
                jsonl_file,
                timeout=1200,
                client=storage_client
            )

            logging.info(f"Upload successful: {blob_path}")

        logging.info("All files exported successfully")
        print("SUCCESS: Export completed.")

    except Exception as e:
        logging.error(f"ERROR in export_to_gcs: {str(e)}")
        print("FAILED. Check export_to_gcs.log.")


if __name__ == "__main__":
    export_to_gcs()