from pyspark.sql import SparkSession
import logging
from datetime import datetime
from scripts.common import load_schema, parse_schema, write_data


class BronzeLayer:
    def __init__(self, spark):
        self.spark = spark
        self.class_name = self.__class__.__name__

    def read_raw_data(self, file_path):
        """
        Reads raw data from a CSV file into a DataFrame with the specified schema.
        """
        logging.info(f"{self.class_name} - Reading raw data from {file_path}")
        schema = parse_schema(load_schema("../scripts/schema/bronze_schema.json"))

        return self.spark.read.csv(file_path, header=True, schema=schema)

    def clean_data(self, df):

        """
        Performs basic data cleaning on the DataFrame.
        """
        logging.info(f"{self.class_name} - Cleaning data by dropping rows with all null values.")

        # Drop rows with all null values
        df = df.dropna(how='all')

        # Remove duplicates
        df = df.dropDuplicates()

        # Standardize formats
        # df = df.toDF(*[c.lower() for c in df.columns])

        return df

    def write_data(self, df, output_path, partition_column):
        logging.info(f"{self.class_name} - Writing cleaned data to {output_path}, partitioned by {partition_column}.")
        write_data(df, output_path, logging, partition_column='STATEFIP')


if __name__ == "__main__":

    from scripts.validation import Validation

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
        bronze = BronzeLayer(spark)
        raw_df = bronze.read_raw_data(raw_data_path)
        cleaned_df = bronze.clean_data(raw_df)
        Validation.validate_bronze(cleaned_df)
        bronze.write_data(cleaned_df, bronze_output_path, "STATEFIP")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped.")

