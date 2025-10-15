# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# Create a Spark session
spark = (SparkSession.builder
         .appName("Task3_Windowed_Analytics")
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
).withWatermark("timestamp", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_aggregation = timestamp_stream.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)

# Extract window start and end times as separate columns
windowed_with_time = windowed_aggregation.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

def write_batch_to_csv(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    if batch_df.count() > 0:  # Only write if batch has data
        output_path = f"outputs/task3/batch_{batch_id}"
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f"Batch {batch_id} written to {output_path} with {batch_df.count()} records")
    else:
        print(f"Batch {batch_id} is empty (waiting for window to complete)")
    
# Use foreachBatch to apply the function to each micro-batch
query = windowed_with_time.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_csv) \
    .start()


query.awaitTermination()
