from pyspark.sql import SparkSession
import logging
from datetime import datetime
from scripts.schema import expected_schema
from scripts.data_validation import validate_schema, handle_schema_validation_failure
from scripts.common import write_data

def read_raw_data(spark, file_path):
    """
    Reads raw data from a CSV file into a DataFrame with the specified schema.
    """
    logging.info(f"Reading raw data from {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def clean_data(df):

    """
    Performs basic data cleaning on the DataFrame.
    """
    logging.info("Cleaning data")

    # Drop rows with all null values
    df = df.dropna(how='all')

    # Remove duplicates
    df = df.dropDuplicates()

    # Validate data types
    for field in expected_schema:
        if df.schema[field.name].dataType != field.dataType:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))

    # Standardize formats
    # df = df.toDF(*[c.lower() for c in df.columns])

    return df


if __name__ == "__main__":

    # Configure logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{timestamp}.log"
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(f"../logs/bronze/{log_filename}"),
                                  logging.StreamHandler()])

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Bronze Layer") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Define file paths and schema
    raw_data_path = "../data/raw_data/mhcld_puf_2021.csv"
    bronze_output_path = "../data/bronze"

    try:
        # Read raw data
        df_raw = read_raw_data(spark, raw_data_path)

        # Clean data
        df_cleaned = clean_data(df_raw)

        # Validate schema
        if validate_schema(df_cleaned, expected_schema):
            logging.info("Schema is valid.")
            # Write cleaned data to Bronze layer
            write_data(df_cleaned, bronze_output_path, logging, partition_column='STATEFIP')
        else:
            logging.error("Schema validation failed.")
            handle_schema_validation_failure()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped.")

