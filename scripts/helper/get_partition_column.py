from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, stddev, avg


def get_partition_column(df, threshold=0.5):
    scores = {}
    total_count = df.count()
    target_column, max_val = None, 0

    for column in df.columns:
        # Calculate cardinality
        cardinality = df.select(column).distinct().count()
        if cardinality / total_count > threshold:
            continue

        # Assess data distribution
        partition_counts = df.groupBy(column).count().select('count')
        distribution_stddev = partition_counts.agg(stddev('count')).collect()[0][0]
        distribution_mean = partition_counts.agg(avg('count')).collect()[0][0]

        # Handle NoneType for distribution_stddev
        if distribution_stddev is None:
            distribution_stddev = 0.0

        # Calculate coefficient of variation (stddev / mean)
        if distribution_mean != 0:
            coefficient_of_variation = distribution_stddev / distribution_mean
        else:
            coefficient_of_variation = float('inf')  # Handle division by zero

        # Calculate composite score
        composite_score = (cardinality * 0.5) + ((1 / (1 + coefficient_of_variation)) * 0.5)

        scores[column] = composite_score

        if composite_score > max_val:
            max_val = composite_score
            target_column = column

    return target_column

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PartitioningAnalysis").getOrCreate()

    # Read the dataset
    df = spark.read.csv("../../data/raw_data/mhcld_puf_2021.csv", header=True, inferSchema=True)

    # Calculate scores potential partitioning columns
    partition_column = get_partition_column(df)

    print(partition_column)

