import logging
import os
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from bronze_layer import BronzeLayer
from silver_layer import SilverLayer
from gold_layer import GoldLayer
from scripts.validation import Validation
from scripts.download import Download

# Configure logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{timestamp}.log"
logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(f"../logs/{log_filename}"),
                                  logging.StreamHandler()])
logger = logging.getLogger()

def main():

    logger.info("Starting the data processing pipeline.", extra={'classname': 'Main'})

    # Increase timeout and resource allocation settings
    conf = SparkConf()
    conf.set("spark.network.timeout", "800s")
    conf.set("spark.executor.heartbeatInterval", "100s")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.driver.cores", "2")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Main") \
        .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    validation = Validation()

    try:
        # File paths
        raw_data_path = "../data/raw_data/mhcld_puf_2021.csv"
        bronze_output_path = "../data/bronze"
        silver_output_path = "../data/silver"
        gold_output_path = "../data/gold"

        # Check if raw data exist
        if os.path.exists(raw_data_path):
            logger.info(f"The file {raw_data_path} exists.")
        else:
            logger.info(f"The file {raw_data_path} does not exist, start to download file")
            unzip_path = '/'.join(raw_data_path.split('/')[:-1])
            download = Download(unzip_path)
            download.download()

        # Bronze Layer
        bronze = BronzeLayer(spark)
        raw_df = bronze.read_raw_data(raw_data_path)
        cleaned_df = bronze.clean_data(raw_df)
        validation.validate_bronze(cleaned_df)
        bronze.write_data(cleaned_df, bronze_output_path, "STATEFIP")

        # Silver Layer
        silver = SilverLayer(spark)
        target_column = ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY']
        converted_df = silver.convert_columns_to_string(cleaned_df, target_column)
        df_dict = silver.partition_and_sample_data(converted_df, target_column)
        validation.validate_silver(df_dict['training'])
        validation.validate_silver(df_dict['testing'])
        validation.validate_silver(df_dict['validation'])
        # # Save transformed data to silver layer
        silver.write_data(df_dict, silver_output_path, target_column)

        # Gold Layer
        gold = GoldLayer(spark)
        train_df, test_df, validation_df = df_dict['training'], df_dict['testing'], df_dict['validation']

        # Define the dataset functions and types
        dataset_functions = {
            "patient_demographics": gold.create_patient_demographics_dataset,
            "mental_health_diagnosis": gold.create_mental_health_diagnosis_dataset,
            "service_utilization": gold.create_service_utilization_dataset,
            "outcome_and_label": gold.create_outcome_and_label_dataset
        }

        # Define the splits
        splits = {
            "training": train_df,
            "testing": test_df,
            "validation": validation_df
        }

        # Loop over the datasets and call the corresponding functions
        for dataset_name, func in dataset_functions.items():
            for split_name, split_df in splits.items():
                func(split_df, f"{gold_output_path}/{dataset_name}/{split_name}")

        logger.info("Data processing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Data processing pipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()