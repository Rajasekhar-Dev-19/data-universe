from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,IntegerType,StringType

spark = SparkSession.builder.appName("Read and Write on CSV file").getOrCreate()

"""
PySpark supports reading a CSV file with a pipe, comma, tab, space, or any other delimiter/separator files.
PySpark reads CSV files in parallel, leveraging multiple executor nodes to accelerate data ingestion.
PySpark can automatically infer the schema of CSV files, eliminating the need for manual schema definition in many cases.
Users have the flexibility to define custom schemas for CSV files, specifying data types and column names as needed.
PySpark offers options for handling headers in CSV files, allowing users to skip headers or treat them as data rows.
Provides robust error handling mechanisms for dealing with malformed or corrupted CSV files, ensuring data integrity.

Filtering: Selecting rows from the DataFrame based on specified conditions.
Selecting Columns: Extracting specific columns from the DataFrame.
Adding Columns: Creating new columns by performing computations or transformations on existing columns.
Dropping Columns: Removing unnecessary columns from the DataFrame.
Grouping and Aggregating: Grouping rows based on certain criteria and computing aggregate statistics, such as sum, average, count, etc., within each group.
Sorting: Arranging the rows of the DataFrame in a specified order based on column values.
Joining: Combining two DataFrames based on a common key or condition.
Union: Concatenating two DataFrames vertically, adding rows from one DataFrame to another.
Pivoting and Melting: Reshaping the DataFrame from long to wide format (pivoting) or from wide to long format (melting).
Window Functions: Performing calculations over a sliding window of rows, such as computing moving averages or ranking.

header: Specifies whether to include a header row with column names in the CSV file. Example: option("header", "true").
delimiter: Specifies the delimiter to use between fields in the CSV file. Example: option("delimiter", ",").
quote: Specifies the character used for quoting fields in the CSV file. Example: option("quote", "\"").
escape: Specifies the escape character used in the CSV file. Example: option("escape", "\\").
nullValue: Specifies the string to represent null values in the CSV file. Example: option("nullValue", "NA").
dateFormat: Specifies the date format to use for date columns. Example: option("dateFormat", "yyyy-MM-dd").
mode: Specifies the write mode for the output. Options include “overwrite”, “append”, “ignore”, and “error”. Example: option("mode", "overwrite").
compression: Specifies the compression codec to use for the output file. Example: option("compression", "gzip").


1.Number of Input Files: Each file in the input directory can be read as a separate partition. 
If you have multiple CSV files in a directory, each file will typically correspond to one partition.

2.File Size: The size of the CSV file can also influence the number of partitions. If a file is large enough, Spark may split it into multiple partitions. 
The default block size in HDFS (Hadoop Distributed File System) is typically 128 MB or 256 MB, depending on the configuration. 
If your CSV file exceeds this size, it may be split into multiple partitions.

3.Spark Configuration: The default number of partitions can also be influenced by Spark's configuration settings. 
For example, the spark.sql.files.maxPartitionBytes configuration controls the maximum number of bytes to pack into a single partition when reading files. 
The default value is 128 MB.

Way1: df.write.csv("path/to/output/directory")
Way2: df.write.csv("path/to/output/directory", options)
Way3: df.write.option("header", "true").csv("path/to/output/directory")
Way4: df.write.option("header", "true").option("sep", ";").csv("path/to/output/directory")
Way5: df.write.mode("overwrite").option("header", "true").csv("path/to/output/directory")
Way6: df.write.partitionBy("column_name").option("header", "true").csv("path/to/output/directory")
Way7: df.write.option("header", "true").option("compression", "gzip").csv("path/to/output/directory")
Way8: df.coalesce(1).write.option("header", "true").csv("path/to/output/directory")
Way9: df2.write.format("csv").mode('overwrite').save("/tmp/spark_output/zipcodes")
"""

file_path="F:/Bigdata/Datasets/emp.txt"
emp_file_path = "F:/Bigdata/Datasets/emp_window_functions.txt"
# Way-1
custom_schema = StructType([
    StructField("emp_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True),
])

# Way-2
custom_schema1 = (StructType()
                  .add("emp_id",IntegerType(),True)
                  .add("name",StringType(),True)
                  .add("age",StringType(),True)
                  .add("gender",StringType(),True)
                  .add("salary",IntegerType(),True)
                  )
# Way-1
# df = spark.read.format("csv").schema(custom_schema).load(file_path)
# Way-2
# df = spark.read.format("csv").schema(custom_schema1).load(file_path)
# df.show()
# df.printSchema()

# PartitionBy
emp_output_file_path = "D:/Data/Emp/default_part/"
emp_output_file_path_wt_headers = "D:/Data/Emp/default_part_wt_headers/"
emp_output_file_path_wt_headers_1 = "D:/Data/Emp/default_part_wt_headers_1/"
emp_output_file_path_wt_headers_sep = "D:/Data/Emp/default_part_wt_headers_sep/"
emp_output_file_path_wt_headers_sep_1 = "D:/Data/Emp/default_part_wt_headers_sep_1/"
emp_output_file_path_wt_headers_repart = "D:/Data/Emp/default_part_wt_headers_repart/"
emp_output_file_path_wt_headers_cust_part = "D:/Data/Emp/default_part_wt_headers_cust_part/"
emp_output_file_path_wt_headers_bucket = "D:/Data/Emp/default_part_wt_headers_bucket/"
emp_output_file_path_wt_headers_parquet = "D:/Data/Emp/default_part_wt_headers_parquet/"
emp_output_file_path_wt_headers_parquet_1 = "D:/Data/Emp/default_part_wt_headers_parquet_1/"
emp_df = spark.read.csv(emp_file_path,custom_schema)
# emp_df.printSchema()
# print(emp_df.rdd.getNumPartitions())
"""
Way-1: if we want save the output data into a csv format with default partition we need to use bellow syntax.
Syntax : emp_df.write.csv(emp_output_file_path) 
Way-2: If we want save data into csv format with headers and default partitions we need to use bellow syntax.
Syntax-A: emp_df.write.csv(emp_output_file_path_wt_headers,header=True)
Syntax-B: emp_df.write.option("header",True).csv(emp_output_file_path_wt_headers_1)
Way-3: If we want save data into csv format with headers and seperator(delimiter) and default partitions.
Syntax-A: emp_df.write.option("header",True).option("sep","#").csv(emp_output_file_path_wt_headers_sep)
Syntax-B: emp_df.write.options(header=True,sep="#").csv(emp_output_file_path_wt_headers_sep_1)
Save data with the different modes [We have four modes to save the data]
1. error : it's default mode, if directory already exists simply throw an error "folder already exists"
2. ignore : we need to pass this argument to mode method, if file already there it will ignore the save operation.
3. append : it will append data to existing data, which will be duplicate if we run program couple of times on same date
4. overwrite : first it will drop the directory and recreate the directory and save the data.

# partitioning
repartition : it's near equally distribute the data among all partitions, we can increase/decrease partitions with 
repartition transformation. it's a bit expensive shuffle transformation when compare to coalesce, because it is first 
drop the partitions and recreate the partitions, while recreate the partitions it involves the reshuffle the data 
between executors. But repartition is one of the performance optimization technique  

Custom Partition : instead of default partition, we can choose the custom partition as well. 
we will pick a column from the dataframe we will use it as partition column, then data will be segregated based on the 
column values. 99% it might not be equally distribute the data, we can chose one or more columns as a partition columns.
if we use more than one column first column will become the parent directory under the given directory rest of the 
directories will become the child of child directories. custom partition will exclude the column from the data and 

Note: The best recommended column for partitioning is low cardinal values which means which column is having more duplicates.
Limitation : partitions will emit the data skew, salting is best technique to overcome the data skewness.

Bucketing : storage and query performance, particularly for large datasets. The logic behind bucketBy revolves around 
organizing data into a fixed number of buckets based on the hash of one or more specified columns.
Hashing:
When we use bucketBy, Spark applies a hash function to the specified column(s) of the DataFrame. 
This hash function generates a hash value for each row based on the values in the specified column(s).
The hash value is then used to determine which bucket the row will belong to. Specifically, 
the bucket number is calculated as hash_value % num_buckets, where num_buckets is the number of buckets you specified
Fixed Number of Buckets:
The number of buckets is defined upfront and remains constant. 
This means that the data will be distributed across these buckets based on the hash values of the specified columns.
Each bucket is stored as a separate file in the underlying file system (e.g., HDFS, S3).
Data Co-location:
Rows that have the same value in the bucketed column(s) will end up in the same bucket. 
This co-location of related data can significantly improve the performance of certain operations, 
such as joins and aggregations, because Spark can minimize data shuffling.
Sorting Within Buckets:
You can also specify sorting within each bucket using the sortBy option. 
This means that the data within each bucket will be sorted based on the specified column(s), 
which can further optimize query performance.
Performance Optimization:
Reduced Shuffling: When performing join operations on bucketed tables, Spark can avoid shuffling data across the network
,as related data is already colocated in the same bucket. This leads to faster execution times.
Efficient Query Execution: When querying a bucketed table, Spark can skip irrelevant buckets, 
leading to faster query performance. This is known as bucket pruning.
Improved Data Skew Handling:
Bucketing can help mitigate data skew by distributing data more evenly across buckets, 
especially when dealing with high cardinality columns. This can lead to more balanced workloads across the cluster.
Optimized Storage:
By organizing data into buckets, you can optimize storage and retrieval patterns, 
making it easier to manage large datasets.
Compatibility with Hive:
Bucketing in Spark is compatible with Hive's bucketing scheme, allowing for seamless integration when working with Hive tables.
"""
# emp_df.write.csv(emp_output_file_path)
# emp_df.write.csv(emp_output_file_path_wt_headers,header=True)
# emp_df.write.option("header",True).csv(emp_output_file_path_wt_headers_1)
# emp_df.write.option("header",True).option("sep","#").csv(emp_output_file_path_wt_headers_sep)
# emp_df.write.options(header=True,sep="#").csv(emp_output_file_path_wt_headers_sep_1)
# emp_df.write.mode("error").csv(emp_output_file_path)
# emp_df.write.mode("ignore").csv(emp_output_file_path)
# emp_df.write.mode("append").csv(emp_output_file_path)
# emp_df.write.mode("overwrite").csv(emp_output_file_path)
# repartition
# emp_df_1 = emp_df.repartition(2)
# emp_df_1.write.mode("overwrite").csv(emp_output_file_path_wt_headers_repart)
# Custom partitions
# emp_df.write.partitionBy("gender").mode("overwrite").csv(emp_output_file_path_wt_headers_cust_part)
# emp_df.write.mode("overwrite").parquet(emp_output_file_path_wt_headers_parquet)
# emp_df.write.partitionBy("gender").mode("overwrite").parquet(emp_output_file_path_wt_headers_parquet_1)
# coalesce

# Bucketing
# emp_df.write.bucketBy(2,"emp_id").mode("overwrite").parquet(emp_output_file_path_wt_headers_bucket)


# emp_df = spark.read.csv(emp_file_path,custom_schema)
# print("Repartition")
# print(emp_df.rdd.getNumPartitions())
# emp_df_1 = emp_df.repartition(2)
# print(emp_df_1.rdd.getNumPartitions())
# emp_df_2 = emp_df.repartition(1)
# print(emp_df_2.rdd.getNumPartitions())
#
# print("Coalesce")
# print(emp_df.rdd.getNumPartitions())
# emp_df_c1 = emp_df.coalesce(2)
# print(emp_df_c1.rdd.getNumPartitions())
# emp_df_c2 = emp_df_1.coalesce(1)
# print(emp_df_c2.rdd.getNumPartitions())

# Indian Pincodes
# pincodes_file_path = "D:/Data/India_pincodes/pincodes.csv"
# raw_df = spark.read.csv(pincodes_file_path,header=True, inferSchema=True)
# raw_df.printSchema()

# raw_df.show(5)
bad_data_file_path = "../data/bad.txt"

# input_df = spark.read.csv(bad_data_file_path,header=True, inferSchema=True)
# input_df = spark.read.format("csv").option("header","true").option("inferSchema","true") \
#     .option("mode","PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").load(bad_data_file_path)
# input_df.printSchema()
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    # StructField("_corrupt_record", StringType(), True)  # Optional: helps with schema alignment
])

# input_df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("mode", "PERMISSIVE") \
#     .option("columnNameOfCorruptRecord", "_corrupt_record") \
#     .schema(schema) \
#     .load(bad_data_file_path)

# input_df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("mode", "DROPMALFORMED") \
#     .schema(schema) \
#     .load(bad_data_file_path)
# input_df.show()

input_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "FAILFAST") \
    .schema(schema) \
    .load(bad_data_file_path)
input_df.show()