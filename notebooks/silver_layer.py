import json
import logging
from datetime import datetime
from pyspark.sql.functions import col, when, lit, concat_ws, format_number
from pyspark.sql import SparkSession
from scripts.common import write_data
# from pyspark.ml.feature import MinMaxScaler, StandardScaler
# from pyspark.ml.linalg import Vectors

class SilverLayer:
    def __init__(self, spark):
        self.spark = spark

    def read_config(self, file_path):
        logging.info(f"Get convert configfrom : {file_path}", extra={'classname': self.__class__.__name__})
        with open(file_path, 'r') as f:
            return json.load(f)


    def convert_columns_to_string(self, df, converted_columns):
        """
        Performs data type transformation num => string
        """
        logging.info(f"Conveted data type: {str(converted_columns)}", extra={'classname': self.__class__.__name__})
        config_path = "../config/data_type.json"
        config = self.read_config(config_path)

        for column in converted_columns:
            mapping = config[column]
            df = df.withColumn(column, col(column).cast("string"))
            for k, v in mapping.items():
                df = df.withColumn(column, when(col(column) == str(k), v).otherwise(col(column)))

        return df

    def partition_and_sample_data(self, df, stratified_columns, train_fraction=0.8, validation_fraction=0.1):
        logging.info(f"Sample data to train, test and validation", extra={'classname': self.__class__.__name__})
        # Create a new stratified column containing the combination of all stratified columns
        df = df.withColumn("stratified_col", concat_ws("_", *stratified_columns))


        # Set sampling fractions
        fractions_df = df.select("stratified_col").distinct().withColumn("fraction", lit(train_fraction))
        fractions = {row["stratified_col"]: row["fraction"] for row in fractions_df.collect()}

        # Perform stratified sampling to get the training set
        train_df = df.stat.sampleBy("stratified_col", fractions, seed=12345)

        # The remaining data is used as the test set
        test_df = df.subtract(train_df)

        # Set sampling fractions for the validation set within the training set
        train_fractions_df = train_df.select("stratified_col").distinct().withColumn("fraction", lit(validation_fraction))
        train_fractions = {row["stratified_col"]: row["fraction"] for row in train_fractions_df.collect()}

        # Perform stratified sampling to get the validation set
        validation_df = train_df.stat.sampleBy("stratified_col", train_fractions, seed=12345)

        # Update the training set by removing the validation set portion
        train_df_updated = train_df.subtract(validation_df)

        # Drop the temporary stratified column
        train_df_updated = train_df_updated.drop("stratified_col")
        test_df = test_df.drop("stratified_col")
        validation_df = validation_df.drop("stratified_col")

        df_dict = {
            "training": train_df_updated,
            "testing": test_df,
            "validation": validation_df
        }

        return df_dict

    def check_proportions(self, train_df, test_df, validation_df, stratified_columns):

        # Calculate the proportion of each subgroup in the training dataset
        train_counts = train_df.groupBy(*stratified_columns).count().withColumnRenamed("count", "train_count")
        train_total_count = train_df.count()
        train_counts = train_counts.withColumn("train_proportion", format_number(train_counts["train_count"] * 100 / train_total_count, 2))

        # Calculate the proportion of each subgroup in the testing dataset
        test_counts = test_df.groupBy(*stratified_columns).count().withColumnRenamed("count", "test_count")
        test_total_count = test_df.count()
        test_counts = test_counts.withColumn("test_proportion", format_number(test_counts["test_count"] * 100 / test_total_count, 2))

        # Calculate the proportion of each subgroup in the original dataset
        validation_counts = validation_df.groupBy(*stratified_columns).count().withColumnRenamed("count", "validation_count")
        total_count = validation_df.count()
        validation_counts = validation_counts.withColumn("validation_proportion",
                                                     format_number(validation_counts["validation_count"] * 100 / total_count,
                                                                   2))

        # Merge results for comparison
        proportions_df = train_counts.join(test_counts, stratified_columns, "outer").join(validation_counts,
                                                                                              stratified_columns, "outer")
        proportions_df.orderBy(col("train_count").desc()).show(truncate=False)

    def write_data(self, df_dict, silver_output_path, partition_column):
        logging.info(f"Writing data to {silver_output_path}, partitioned by {partition_column}",
                     extra={'classname': self.__class__.__name__})
        for name, df in df_dict.items():
            write_data(df, f"{silver_output_path}/{name}", logging, partition_column=partition_column)

    # def convert_columns_to_float(self, df, cloumn):
    #     """
    #     Performs data type transformation num => float
    #     """
    #
    #     return df

    # def normalize_and_standardize_columns(self, df, columns):
    #
    #     for column in columns:
    #         # Change to vector
    #         df = df.withColumn(f"{column}_vector", Vectors.dense(df[column]))
    #
    #         # normalize
    #         min_max_scaler = MinMaxScaler(inputCol=f"{column}_vector", outputCol=f"{column}_normalized")
    #         min_max_model = min_max_scaler.fit(df)
    #         df = min_max_model.transform(df).drop(f"{column}_vector")
    #
    #         # Change to vector
    #         df = df.withColumn(f"{column}_vector", Vectors.dense(df[column]))
    #
    #         # standardize
    #         standard_scaler = StandardScaler(inputCol=f"{column}_vector", outputCol=f"{column}_standardized", withStd=True, withMean=True)
    #         standard_model = standard_scaler.fit(df)
    #         df = standard_model.transform(df).drop(f"{column}_vector")
    #
    #     return df

if __name__ == "__main__":

    # Configure logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{timestamp}.log"
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(f"../logs/silver/{log_filename}"),
                                  logging.StreamHandler()])

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    # Define file paths
    silver_output_path = "../data/silver"

    # Load cleaned data
    df = spark.read.parquet("../data/bronze")

    # Silver Layer
    silver = SilverLayer(spark)

    # 1-1. Data Type Conversion
    # Didn't find 'INCOME'
    conveted_columns = ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY']
    df = silver.convert_columns_to_string(df, conveted_columns)

    # 1-2. Data Type Conversion
    # Not able to find columns in dataset
    # columns = ['MENHLTH', 'PHYHLTH', 'POORHLTH']
    # df = silver.convert_columns_to_float(df, columns)

    # 2. Normalize & Standardize
    # columns = ['MENHLTH', 'PHYHLTH', 'POORHLTH']
    # df = silver.normalize_and_standardize_columns(df, columns)

    # 3. Split data into training, testing, validation
    stratified_columns = ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY']
    df_dict = silver.partition_and_sample_data(df, stratified_columns)

    # Portion check
    # check_proportions(df, df_dict['training'], df_dict['testing'], stratified_columns)

    # # Save transformed data to silver layer
    silver.write_data(df_dict, silver_output_path, "STATEFIP")