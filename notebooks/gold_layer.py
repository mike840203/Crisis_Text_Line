from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum


def create_aggregated_services_table(df, output_path):
    aggregated_services_df = df.groupBy("YEAR", "GENDER", "RACE", "AGE", "STATEFIP").agg(
        sum("SPHSERVICE").alias("Total_Services"),
        count("SPHSERVICE").alias("Service_Type_Count")
    )

    aggregated_services_df.write.partitionBy("STATEFIP").mode("overwrite").parquet(output_path)


def create_health_outcomes_table(df, output_path):
    health_outcomes_df = df.select("YEAR", "AGE", "GENDER", "RACE", "ETHNIC", "STATEFIP", "ANXIETYFLG", "DEPRESSFLG")
    health_outcomes_df.write.partitionBy("STATEFIP").mode("overwrite").parquet(output_path)


def create_service_utilization_table(df, output_path):
    service_utilization_df = df.groupBy("YEAR", "EMPLOY", "STATEFIP").agg(
        sum("SPHSERVICE").alias("Total_Services"),
        count("SPHSERVICE").alias("Service_Type_Count")
    )
    service_utilization_df.write.partitionBy("STATEFIP").mode("overwrite").parquet(output_path)


if __name__ == "__main__":

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Define file paths
    gold_output_path = "../data/gold"

    # Load transformed data
    train_df = spark.read.parquet("../data/silver/training")
    test_df = spark.read.parquet("../data/silver/testing")
    validation_df = spark.read.parquet("../data/silver/validation")

    # Aggregated Services Table
    create_aggregated_services_table(train_df, f"{gold_output_path}/aggregated_services/train")
    create_aggregated_services_table(test_df, f"{gold_output_path}/aggregated_services/test")
    create_aggregated_services_table(validation_df, f"{gold_output_path}/aggregated_services/validation")

    # Demographics and Health Outcomes Table
    create_health_outcomes_table(train_df, f"{gold_output_path}/health_outcomes/train")
    create_health_outcomes_table(test_df, f"{gold_output_path}/health_outcomes/test")
    create_health_outcomes_table(validation_df, f"{gold_output_path}/health_outcomes/validation")

    # Service Utilization by Employment Table
    create_service_utilization_table(train_df, f"{gold_output_path}/service_utilization/train")
    create_service_utilization_table(test_df, f"{gold_output_path}/service_utilization/test")
    create_service_utilization_table(validation_df, f"{gold_output_path}/service_utilization/validation")