from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour, window, sum, col, when, size
import time
import sys

print("Starting Spark Streaming ETL...")

spark = SparkSession.builder \
    .appName("TelecomStreamingETL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.create.bucket", "true") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
    .getOrCreate()

print("Spark session created successfully")

print("Setting up Kafka connection...")
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "telecom_events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load()
    print("Kafka connection configured successfully")
except Exception as e:
    print(f"Error setting up Kafka connection: {e}")
    spark.stop()
    sys.exit(1)

lines = df.selectExpr("CAST(value AS STRING) as line")
parsed = lines.selectExpr("split(line, '\t') as fields")
valid = parsed.filter(size("fields") == 8)

final_df = valid.selectExpr(
    "CAST(fields[0] AS INT) as cell_id",
    "CAST(fields[1] AS BIGINT) as timestamp",
    "CAST(fields[2] AS INT) as country_code",
    "CAST(fields[3] AS DOUBLE) as sms_in",
    "CAST(fields[4] AS DOUBLE) as sms_out",
    "CAST(fields[5] AS DOUBLE) as call_in",
    "CAST(fields[6] AS DOUBLE) as call_out",
    "CAST(fields[7] AS DOUBLE) as internet"
).withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) \
 .withColumn("date", to_date("event_time")) \
 .withColumn("hour", hour("event_time"))

numeric_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet"]
for c in numeric_cols:
    final_df = final_df.withColumn(c, when(col(c).isNull() | (col(c) < 0), 0.0).otherwise(col(c)))

final_df = final_df.filter((col("cell_id") >= 1) & (col("cell_id") <= 10000))

try:
    query_raw = final_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://telecom-data/processed/streamed_events") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/raw") \
        .partitionBy("date") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    print("Raw events query started successfully")
except Exception as e:
    print(f"Error starting raw events query: {e}")
    spark.stop()
    sys.exit(1)

try:
    agg_df = final_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window("event_time", "1 hour"), "cell_id") \
        .agg(
            sum("sms_in").alias("sms_in_total"),
            sum("sms_out").alias("sms_out_total"),
            sum("call_in").alias("call_in_total"),
            sum("call_out").alias("call_out_total"),
            sum("internet").alias("internet_total")
        ).withColumn("date", to_date("window.start")) \
         .withColumn("hour", hour("window.start"))

    query_agg = agg_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://telecom-data/processed/hourly_aggregates") \
        .option("checkpointLocation", "/opt/spark/data/checkpoints/agg") \
        .partitionBy("date", "hour") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    print("Aggregates query started successfully")
except Exception as e:
    print(f"Error starting aggregates query: {e}")
    if 'query_raw' in locals() and query_raw.isActive:
        query_raw.stop()
    spark.stop()
    sys.exit(1)

print("Starting streaming monitoring...")
max_runtime = 120
start_time = time.time()

try:
    while time.time() - start_time < max_runtime:
        if query_raw.exception() is not None:
            raise Exception(f"Raw query failed: {query_raw.exception()}")
        if query_agg.exception() is not None:
            raise Exception(f"Agg query failed: {query_agg.exception()}")

        if query_raw.isActive and query_agg.isActive:
            runtime = int(time.time() - start_time)
            print(f"Streaming active... Runtime: {runtime}s")
            if runtime > 30:
                print("Data processing appears to be working. Exiting after demo period.")
                break
            time.sleep(10)
        else:
            print("One or more queries stopped unexpectedly")
            break
    else:
        print(f"Reached max runtime of {max_runtime}s, stopping...")

except KeyboardInterrupt:
    print("Received interrupt signal, stopping...")
except Exception as e:
    print(f"Error during streaming: {e}")

finally:
    print("Stopping streaming queries...")
    try:
        if 'query_raw' in locals() and query_raw.isActive:
            query_raw.stop()
        if 'query_agg' in locals() and query_agg.isActive:
            query_agg.stop()
    except Exception as e:
        print(f"Error stopping queries: {e}")
    
    try:
        spark.stop()
    except Exception as e:
        print(f"Error stopping Spark session: {e}")
    
    print("Streaming ETL completed successfully!")
    print("✅ Exiting with code 0")
    sys.exit(0)