from scripts.helper.get_partition_column import get_partition_column
def write_data(df, output_path, logging, partition_column=None):
    """
    Writes the DataFrame to a specified path, partitioning by the specified column.
    """
    logging.info(f"Writing data to {output_path}, partitioned by {partition_column}")

    if not partition_column:
        logging.info(f"No partition column")
        partition_column = get_partition_column(df)
        logging.info(f"Partition column: {partition_column}")

    df.write.partitionBy(partition_column).mode("overwrite").parquet(output_path)