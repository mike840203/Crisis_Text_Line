from pyspark.sql import SparkSession


class TestCase:

    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Data Ingestion") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .getOrCreate()
        self.df = None

    def load_data(self, table_type, data_type=None, table_name=None):

        if data_type is None:
            data_type = 'training'
        if table_name is None:
            table_name = 'aggregated_services'

        if table_type == 'bronze':
            self.df = self.spark.read.parquet("../data/bronze")
        elif table_type == 'silver':
            self.df = self.spark.read.parquet(f"../data/silver/{data_type}")
        elif table_type == 'gold':
            self.df = self.spark.read.parquet(f"../data/gold/{table_name}/{data_type}")
        else:
            raise ValueError("Invalid data type. Please select from 'bronze', 'silver', or 'gold'.")

    def create_temp_view(self):

        self.df.createOrReplaceTempView("table")

    def run_query(self, sql_query):

        return self.spark.sql(sql_query)


if __name__ == "__main__":

    test = TestCase()
    test.load_data('gold', 'train', 'service_utilization')
    test.create_temp_view()

    with open("query.sql", "r") as f:
        query = f.read()

    result = test.run_query(query)

    result.printSchema()
    result.show()
