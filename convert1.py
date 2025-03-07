import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# API URL (Replace with actual API endpoint)
API_URL = "people-1000.csv"  
PARQUET_FILE_PATH = "people.parquet"

def fetch_employee_data(api_url):
    #Fetch employee data from API and return as DataFrame.
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise error for bad responses
        data = response.json()  # Assuming the response is in JSON format

        df = pd.DataFrame(data)  # Convert JSON data to DataFrame
        logging.info("Successfully fetched data from API.")

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def convert_to_parquet(df, parquet_path):
    #Convert DataFrame to Parquet.
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        logging.info("Parquet file successfully generated at: %s", parquet_path)
    except Exception as e:
        logging.error(f"Error converting data to Parquet: {e}")

def lambdaHandler(event, context):
    #AWS Lambda handler function.
    try:
        logging.info("Fetching data from API...")
        df = fetch_employee_data(API_URL)

        if df is not None:
            convert_to_parquet(df, PARQUET_FILE_PATH)
            logging.info("Lambda function executed successfully.")
        else:
            logging.error("No data to process.")

    except Exception as e:
        logging.error(f"Error in lambda function: {e}")

# Run the process if executed directly
if __name__ == "__main__":
    df = fetch_employee_data(API_URL)
    if df is not None:
        convert_to_parquet(df, PARQUET_FILE_PATH)
