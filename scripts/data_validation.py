from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField


def normalize_schema(schema):
    """
    Normalize the schema by converting all field names to lowercase.
    """
    return StructType([StructField(field.name.lower(), field.dataType, field.nullable, field.metadata) for field in schema])


def validate_schema(df, expected_schema):

    normalized_df_schema = normalize_schema(df.schema)
    normalized_expected_schema = normalize_schema(expected_schema)

    return normalized_df_schema == normalized_expected_schema


def handle_schema_validation_failure():
    print("Schema validation failed. Aborting processing.")


if __name__ == "__main__":

    from schema import expected_schema

    spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    df = spark.read.schema(expected_schema).csv("../data/raw_data/mhcld_puf_2021.csv", header=True)

    if validate_schema(df, expected_schema):
        print("Schema is valid.")
    else:
        handle_schema_validation_failure()

