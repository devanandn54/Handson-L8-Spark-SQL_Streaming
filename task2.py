from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- Define the function to process each micro-batch ---
def write_batch_to_csv(batch_df, batch_id):
    """
    Writes a micro-batch DataFrame to a CSV file.
    The filename will include the batch ID to ensure uniqueness.
    """
    # Print a message to the console for monitoring
    print(f"--- Processing batch ID: {batch_id} ---")
    if not batch_df.rdd.isEmpty():
        # Define the output path. The data will be saved in a folder with the batch ID.
        output_path = f"outputs/task2/driver_stats_batch_{batch_id}"
        
        # Save the batch DataFrame as a CSV file with the batch ID in the filename
        # Use .coalesce(1) to write to a single CSV file instead of multiple part-files
        (batch_df.coalesce(1).write
         .mode("overwrite")
         .option("header", "true")
         .csv(output_path))
        print(f"Successfully wrote batch {batch_id} to {output_path}")

# --- Main Spark Application ---

# Create a Spark session
spark = (SparkSession.builder
         .appName("Task2_Driver_Aggregations_Corrected")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = (spark.readStream
              .format("socket")
              .option("host", "localhost")
              .option("port", 9999)
              .load())

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
timestamp_stream = parsed_stream.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
).withWatermark("timestamp", "10 minutes")

# Compute aggregations: total fare and average distance grouped by driver_id
driver_aggregations = (timestamp_stream
                       .groupBy("driver_id")
                       .agg(
                           sum("fare_amount").alias("total_fare"),
                           avg("distance_km").alias("average_distance")
                       ))

# Use foreachBatch to apply the function to each micro-batch
query = (driver_aggregations.writeStream
         .foreachBatch(write_batch_to_csv)
         .outputMode("update") # Use 'update' mode for aggregations with watermark
         .option("checkpointLocation", "checkpoints/driver_aggregations")
         .start())

# Wait for the streaming query to terminate
query.awaitTermination()
