from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# Initialize a Spark session
spark = SparkSession.builder.appName("CustomerProcessing").getOrCreate()

# Example: Read your Delta table with unique customers
# Replace "your_delta_table_path" with the actual path to your Delta table
delta_table_path = "s3a://your_s3_bucket_path/delta_table"
delta_table_df = spark.read.format("delta").load(delta_table_path)

# Example: Read your main customers table
# Replace "your_main_customers_table_path" with the actual path to your main customers table
main_customers_table_path = "s3a://your_s3_bucket_path/main_customers_table"
main_customers_df = spark.read.format("delta").load(main_customers_table_path)

# Define a window specification to rank changes based on the timestamp
window_spec = Window.partitionBy("customer_id").orderBy(col("change_timestamp").desc())

# Rank the changes for each customer based on the timestamp
ranked_customers_df = main_customers_df.withColumn("row_num", row_number().over(window_spec))

# Select old records (records other than the latest change)
old_records_df = ranked_customers_df.filter(col("row_num") > 1).drop("row_num")

# Find delta records (changes between the Delta table and old records)
delta_records_df = delta_table_df.join(
    old_records_df,
    on="customer_id",
    how="inner"
)

# Example: Show the delta records
delta_records_df.show()

# Example: Write delta records to a new Delta table
# Replace "your_delta_records_path" with the desired path for the Delta records
delta_records_df.write.format("delta").mode("overwrite").save("s3a://your_s3_bucket_path/delta_records")
