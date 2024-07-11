
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("Data Ingestion") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def check_proportions(df, train_df, test_df, stratified_columns):

    # Calculate the proportion of each subgroup in the original dataset
    original_counts = df.groupBy(*stratified_columns).count().withColumnRenamed("count", "original_count")
    total_count = df.count()
    original_counts = original_counts.withColumn("original_proportion", original_counts["original_count"] / total_count)
    print(original_counts)
    # Calculate the proportion of each subgroup in the training dataset
    train_counts = train_df.groupBy(*stratified_columns).count().withColumnRenamed("count", "train_count")
    train_total_count = train_df.count()
    train_counts = train_counts.withColumn("train_proportion", train_counts["train_count"] / train_total_count)
    print(train_counts)
    # Calculate the proportion of each subgroup in the testing dataset
    test_counts = test_df.groupBy(*stratified_columns).count().withColumnRenamed("count", "test_count")
    test_total_count = test_df.count()
    test_counts = test_counts.withColumn("test_proportion", test_counts["test_count"] / test_total_count)
    print(test_counts)
    # Merge results for comparison
    proportions_df = original_counts.join(train_counts, stratified_columns, "outer").join(test_counts, stratified_columns, "outer")
    proportions_df.show(truncate=False)


df_total = spark.read.parquet("../data/bronze")
df_training = spark.read.parquet("../data/silver/training")
df_testing = spark.read.parquet("../data/silver/testing")
df_validation = spark.read.parquet("../data/silver/validation")

print(df_total.count())
print(df_training.count())
print(df_testing.count())
print(df_validation.count())

stratified_columns = ['GENDER', 'RACE', 'ETHNIC', 'MARSTAT', 'EMPLOY']
check_proportions(df_total, df_training, df_testing, stratified_columns)