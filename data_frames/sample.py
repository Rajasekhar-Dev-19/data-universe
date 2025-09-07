from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sample").getOrCreate()
file_path = "..\data\emp_with_header.txt"  # this is one way to read the local file with in project directory
local_file_path="..\data\emp_with_header.txt"

input_df = spark.read.csv(local_file_path, header=True, inferSchema=True)
input_df.show()