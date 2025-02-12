import json

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructField, StructType
from scripts.helper.get_partition_column import get_partition_column


def ensure_partition_column_list(partition_column):
    """
    Ensures that the partition_column is a list. If it is a string, converts it to a list.

    Parameters:
    partition_column (str or list): The partition column(s).

    Returns:
    list: The partition column(s) as a list.
    """
    if isinstance(partition_column, str):
        return [partition_column]
    return partition_column

def write_data(df, output_path, logging, partition_column=None):
    """
    Writes the DataFrame to a specified path, partitioning by the specified column.
    """

    if not partition_column:
        logging.info(f"No partition column")
        partition_column = get_partition_column(df)
        logging.info(f"Partition column: {partition_column}")

    partition_column = ensure_partition_column_list(partition_column)

    df.write.partitionBy(*partition_column).mode("overwrite").parquet(output_path, compression='snappy')

def load_schema(schema_path):
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    return schema

def parse_schema(json_schema):
    fields = []
    for field in json_schema['fields']:
        field_type = field['type']
        if field_type == 'string':
            data_type = StringType()
        elif field_type == 'integer':
            data_type = IntegerType()
        elif field_type == 'double':
            data_type = DoubleType()
        else:
            raise ValueError(f"Unsupported data type: {field_type}")
        fields.append(StructField(field['name'], data_type, field['nullable']))
    return StructType(fields)