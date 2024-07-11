import json

from pyspark.sql.functions import reduce, count, when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from scripts.common import load_schema, parse_schema


def validate_schema(df, schema_json_path):


    json_schema = load_schema(schema_json_path)
    expected_struct = parse_schema(json_schema)
    return df.schema == expected_struct


def validate_bronze(df, path='../scripts/schema/bronze_schema.json'):

    # Schema validation & Data type validation
    if not validate_schema(df, path):
        raise ValueError("Schema does not match the expected schema.")

    # Null value handling
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    # if null_counts.filter(reduce(lambda x, y: x | y, [col(c) > 0 for c in df.columns])).count() > 0:
    #     raise ValueError("Null values found in the DataFrame.")

    print("Bronze validation passed.")
    return True


def validate_silver(df, path='../scripts/schema/silver_schema.json'):
    # Schema validation
    if not validate_schema(df, path):
        raise ValueError("Schema does not match the expected schema.")

    # Data consistency checks
    if df.filter(col("date_column").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")).count() != df.count():
        raise ValueError("Date format is inconsistent.")

    # Data cleansing checks
    if df.filter(col("category_column").isNull()).count() > 0:
        raise ValueError("Category column contains null values.")

    print("Silver validation passed.")
    return True


def validate_gold(df, path='../scripts/schema/gold_schema.json', table_nm=None):
    # Schema validation
    expected_schema = ["final_column1", "final_column2"]
    actual_schema = df.columns
    if set(expected_schema) != set(actual_schema):
        raise ValueError("Schema does not match the expected schema.")

    # Business rule validation
    if df.filter(col("financial_column") < 0).count() > 0:
        raise ValueError("Financial column contains negative values.")

    # Data integrity checks
    if df.join(other_df, "foreign_key").filter(other_df["foreign_key"].isNull()).count() > 0:
        raise ValueError("Data integrity check failed: foreign key constraint violated.")

    print("Gold validation passed.")
    return True