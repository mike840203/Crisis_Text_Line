import json
import logging
from datetime import datetime
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from scripts.common import write_data

def read_config(file_apth):
    with open(file_apth, 'r') as f:
        return json.load(f)


def data_type_transform1(df):
    """
    Performs data type transformation num => string
    """
    # Didn't find 'INCOME'
    columns = ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY']
    config_path = "../config/data_type.json"
    config = read_config(config_path)

    for column in columns:
        mapping = config[column]
        df = df.withColumn(column, col(column).cast("string"))
        for k, v in mapping.items():
            df = df.withColumn(column, when(col(column) == str(k), v).otherwise(col(column)))

    return df


def data_type_transform2(df):
    """
    Performs data type transformation num => float
    """
    columns = ['MENHLTH', 'PHYHLTH', 'POORHLTH']

    # Not able to find columns in dataset

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
        .appName("Data Ingestion") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Define file paths
    silver_output_path = "../data/silver"

    # Load cleaned data
    df = spark.read.parquet("../data/bronze")

    # 1-1. Data Type Conversion
    df = data_type_transform1(df)

    # 1-2. Data Type Conversion
    df = data_type_transform2(df)

    # 2 Normalize & Standardize



    # # Save transformed data to silver layer

    write_data(df, silver_output_path, logging, partition_column='STATEFIP')