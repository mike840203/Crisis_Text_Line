import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, when


class GoldLayer:
    def __init__(self, spark):
        self.spark = spark
        self.class_name = self.__class__.__name__

    def create_patient_demographics_dataset(self, df, output_path):
        logging.info(f"{self.class_name} - Creating patient demographics dataset at: {output_path}")

        patient_demographics_df = df.select(
            "CASEID", "AGE", "GENDER", "RACE", "ETHNIC", "MARSTAT", "EDUC", "VETERAN", "STATEFIP", "REGION", "DIVISION"
        ).withColumn(
            "GENDER", when(col("GENDER") == "Female", 0).otherwise(1)
        ).withColumn(
            "VETERAN", when(col("VETERAN") == "No", 0).otherwise(1)
        )
        # Add one-hot encoding or other transformations as needed

        patient_demographics_df.write.partitionBy("STATEFIP").mode("overwrite").parquet(output_path)

    def create_mental_health_diagnosis_dataset(self, df, output_path):
        logging.info(f"{self.class_name} - Creating mental health diagnosis dataset at: {output_path}")

        mental_health_diagnosis_df = df.select(
            "CASEID", "MH1", "MH2", "MH3", "NUMMHS", "SCHIZOFLG", "DEPRESSFLG", "BIPOLARFLG",
            "ANXIETYFLG", "ADHDFLG", "CONDUCTFLG", "ODDFLG", "DELIRDEMFLG", "PERSONFLG", "PDDFLG", "TRAUSTREFLG",
            "OTHERDISFLG"
        )

        mental_health_diagnosis_df.write.mode("overwrite").parquet(output_path)


    def create_service_utilization_dataset(self, df, output_path):
        logging.info(f"{self.class_name} - Creating service utilization dataset at: {output_path}")

        service_utilization_df = df.select(
            "CASEID", "CMPSERVICE", "IJSSERVICE", "OPISERVICE", "RTCSERVICE", "SPHSERVICE"
        ).withColumn(
            "TOTAL_SERVICES",
            col("CMPSERVICE") + col("IJSSERVICE") + col("OPISERVICE") + col("RTCSERVICE") + col("SPHSERVICE")
        )

        service_utilization_df.write.mode("overwrite").parquet(output_path)

    def create_outcome_and_label_dataset(self, df, output_path):
        logging.info(f"{self.class_name} - Creating outcome and label dataset at: {output_path}")

        # Normalizing age (example of a calculation)
        age_min = df.agg({"AGE": "min"}).collect()[0][0]
        age_max = df.agg({"AGE": "max"}).collect()[0][0]

        outcome_and_label_df = df.select(
            "CASEID", "EMPLOY", "LIVARAG", "DETNLF", "SMISED", "VETERAN", "AGE"
        ).withColumn(
            "AGE_NORMALIZED", (col("AGE") - age_min) / (age_max - age_min)
        )

        outcome_and_label_df.write.mode("overwrite").parquet(output_path)


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

    gold = GoldLayer(spark)

    # Define the dataset functions and types
    dataset_functions = {
        # "patient_demographics": gold.create_patient_demographics_dataset,
        "mental_health_diagnosis": gold.create_mental_health_diagnosis_dataset,
        "service_utilization": gold.create_service_utilization_dataset,
        "outcome_and_label": gold.create_outcome_and_label_dataset
    }

    # Define the splits
    splits = {
        # "training": train_df,
        # "testing": test_df,
        "validation": validation_df
    }

    # Loop over the datasets and call the corresponding functions
    for dataset_name, func in dataset_functions.items():
        for split_name, split_df in splits.items():
            func(split_df, f"{gold_output_path}/{dataset_name}/{split_name}")
