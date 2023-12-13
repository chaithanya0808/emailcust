Certainly! If you have a Delta table with unique customers and want to process each customer against another customers table to identify
those who have changed their phone and address multiple times, and then find the delta records and create an array, you can use the following PySpark code:

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, collect_list

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

# Group by customer_id and collect the delta records into an array
delta_records_array_df = delta_records_df.groupBy("customer_id").agg(
    collect_list(col("change_column")).alias("delta_records_array")
)

# Show the resulting DataFrame
delta_records_array_df.show(truncate=False)

In this example:

We use a window function to rank changes for each customer based on the timestamp in the main customers table.
We select old records by filtering out the latest change for each customer (where row_num > 1).
We find delta records by performing an inner join between the Delta table and the old records on the customer_id.
Finally, we group by customer_id and collect the delta records into an array using collect_list.
Replace placeholders such as "your_s3_bucket_path," "your_delta_table_path," "your_main_customers_table_path," and others with your actual values. Adjust the code based on your specific requirements and schema.
