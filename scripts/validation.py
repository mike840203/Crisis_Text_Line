import logging
from pyspark.sql.functions import count, when, col
from scripts.common import load_schema, parse_schema


class Validation:

    def __init__(self):
        self.class_name = self.__class__.__name__

    def validate_schema(self, df, expected_schema):

        actual_schema = df.schema

        # Check column names
        actual_columns = set([field.name for field in actual_schema.fields])
        expected_columns = set([field.name for field in expected_schema.fields])

        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns

        if missing_columns:
            logging.error(f"{self.class_name} - Missing columns: {missing_columns}")
            return False
        if extra_columns:
            logging.error(f"{self.class_name} - Extra columns: {extra_columns}")
            return False

        # Check column data types and nullability
        for field in expected_schema.fields:
            actual_field = actual_schema[field.name]

            if actual_field.dataType != field.dataType:
                logging.error(f"{self.class_name} - Data type mismatch for column {field.name}: expected {field.dataType}, got {actual_field.dataType}")
                return False

            # if actual_field.nullable != field.nullable:
            #     logging.error(f"Nullability mismatch for column {field.name}: expected {field.nullable}, got {actual_field.nullable}")
            #     return False

        return True


    def validate_bronze(self, df, path='../config/schema/bronze_schema.json'):

        # Schema validation & Data type validation
        if not self.validate_schema(df, parse_schema(load_schema(path))):
            raise ValueError(f"{self.class_name} - Schema does not match the expected schema.")

        # Null value handling
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        # if null_counts.filter(reduce(lambda x, y: x | y, [col(c) > 0 for c in df.columns])).count() > 0:
        #     raise ValueError(f"{self.class_name} - Null values found in the DataFrame.")

        logging.info(f"{self.class_name} - Bronze validation passed.")
        return True


    def validate_silver(self, df, path='../config/schema/silver_schema.json'):

        # Schema validation
        if not self.validate_schema(df, parse_schema(load_schema(path))):
            raise ValueError(f"{self.class_name} - Schema does not match the expected schema.")

        logging.info(f"{self.class_name} - Silver validation passed.")
        return True

