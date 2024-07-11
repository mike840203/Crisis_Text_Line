import logging
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from bronze_layer import BronzeLayer
from silver_layer import SilverLayer
from gold_layer import GoldLayer
from scripts.validation import validate_bronze, validate_silver, validate_gold, load_schema

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
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.driver.cores", "2")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Bronze Layer") \
        .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # File paths
        raw_data_path = "../data/raw_data/mhcld_puf_2021.csv"
        bronze_output_path = "../data/bronze"
        silver_output_path = "../data/silver"
        gold_output_path = "../data/gold"

        # Bronze Layer
        bronze = BronzeLayer(spark)
        raw_df = bronze.read_raw_data(raw_data_path)
        cleaned_df = bronze.clean_data(raw_df)
        validate_bronze(cleaned_df)
        bronze.write_data(cleaned_df, bronze_output_path, "STATEFIP")

        # Silver Layer
        silver = SilverLayer(spark)
        converted_df = silver.convert_columns_to_string(cleaned_df, ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY'])
        df_dict = silver.partition_and_sample_data(converted_df, ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY'])
        validate_silver(df_dict['training'])
        validate_silver(df_dict['testing'])
        validate_silver(df_dict['validation'])
        silver.write_data(df_dict, silver_output_path, "STATEFIP")

        # Gold Layer
        gold = GoldLayer(spark)
        train_df, test_df, validation_df = df_dict['training'], df_dict['testing'], df_dict['validation']
        gold.create_aggregated_services_table(train_df, f"{gold_output_path}/aggregated_services/train")
        gold.create_aggregated_services_table(test_df, f"{gold_output_path}/aggregated_services/test")
        gold.create_aggregated_services_table(validation_df, f"{gold_output_path}/aggregated_services/validation")

        gold.create_health_outcomes_table(train_df, f"{gold_output_path}/health_outcomes/train")
        gold.create_health_outcomes_table(test_df, f"{gold_output_path}/health_outcomes/test")
        gold.create_health_outcomes_table(validation_df, f"{gold_output_path}/health_outcomes/validation")

        gold.create_service_utilization_table(train_df, f"{gold_output_path}/service_utilization/train")
        gold.create_service_utilization_table(test_df, f"{gold_output_path}/service_utilization/test")
        gold.create_service_utilization_table(validation_df, f"{gold_output_path}/service_utilization/validation")

        logger.info("Data processing pipeline completed successfully.", extra={'classname': 'Main'})
    except Exception as e:
        logger.error(f"Data processing pipeline failed: {e}", extra={'classname': 'Main'})
    finally:
        spark.stop()

if __name__ == "__main__":
    main()