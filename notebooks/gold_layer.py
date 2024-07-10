from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Load transformed data
df = spark.read.parquet("data/gold")

# Final transformations (e.g., business logic)
df = df.withColumn("normalized_health_score", (col("menhlth_normalized") + col("phyhlth_normalized") + col("poorhlth_normalized")) / 3)

# Save final data
df.write.mode("overwrite").parquet("data/gold")