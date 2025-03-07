import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# File paths
CSV_FILE_PATH = "people-1000.csv"
PARQUET_FILE_PATH = "people.parquet"


inputDA = {
    "scraper_input": {
        "scraper_name": "csv_100",
        "run_scraper_id": "100"
    }
}

def convert_csv_to_parquet(csv_path, parquet_path):
    try:
        # Load CSV file
        df = pd.read_csv(csv_path)
        logging.info("CSV file successfully loaded!.")

        # Convert DataFrame to Apache Arrow Table
        table = pa.Table.from_pandas(df)

        # Write Parquet file
        pq.write_table(table, parquet_path)
        logging.info("Parquet file successfully generated at: %s", parquet_path)

    except Exception as e:
        logging.error(f"Error converting CSV to Parquet: {e}")

# Lambda handler function
def lambdaHandler(event, context):
    try:
        scraper_name = event.get("scraper_input", {}).get("scraper_name", "unknown_scraper")
        run_scraper_id = event.get("scraper_input", {}).get("run_scraper_id", "unknown_id")

        logging.info(f"Running scraper: {scraper_name} with ID: {run_scraper_id}")

        # Convert CSV to Parquet
        convert_csv_to_parquet(CSV_FILE_PATH, PARQUET_FILE_PATH)

        logging.info("Lambda function executed successfully.")
    except Exception as e:
        logging.error(f"Error in lambda function: {e}")

# Run the conversion if executed directly
if __name__ == "__main__":
    convert_csv_to_parquet(CSV_FILE_PATH, PARQUET_FILE_PATH)
