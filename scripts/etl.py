import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType

# Default paths if not provided via CLI
RAW_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/raw"
S3_BUCKET = sys.argv[2] if len(sys.argv) > 2 else "s3a://telecom-data/processed"


def delete_path_if_exists(spark, path):
    print(f"Attempting to delete path if exists: {path}")
    jvm = spark._jvm
    jsc = spark._jsc
    URI = jvm.java.net.URI
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    conf = jsc.hadoopConfiguration()
    uri = URI(path)
    fs = FileSystem.get(uri, conf)
    p = Path(path)
    if fs.exists(p):
        print(f"Path {path} exists - deleting...")
        fs.delete(p, True)
    else:
        print(f"Path {path} does not exist - skipping delete.")


def create_spark(app_name="milan_batch_etl"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.committer.name", "magic")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


SCHEMA = StructType([
    StructField("cell_id", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("country_code", IntegerType(), True),
    StructField("sms_in", DoubleType(), True),
    StructField("sms_out", DoubleType(), True),
    StructField("call_in", DoubleType(), True),
    StructField("call_out", DoubleType(), True),
    StructField("internet", DoubleType(), True)
])


def load_raw(spark, raw_path):
    return spark.read.csv(f"{raw_path}/*.txt", sep="\t", header=False, schema=SCHEMA)


def transform(df):
    df = df.withColumn("event_time", F.from_unixtime(F.col("timestamp") / 1000))
    df = df.withColumn("date", F.to_date("event_time"))
    df = df.withColumn("hour", F.hour("event_time"))

    numeric_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet"]
    for c in numeric_cols:
        df = df.withColumn(
            c,
            F.when(F.col(c).isNull() | (F.col(c) < 0), 0.0).otherwise(F.col(c))
        )

    df = df.filter((F.col("cell_id") >= 1) & (F.col("cell_id") <= 10000))
    return df


def aggregate_daily(df, s3_bucket):
    agg = df.groupBy("date", "cell_id").agg(
        F.sum("sms_in").alias("sms_in_total"),
        F.sum("sms_out").alias("sms_out_total"),
        F.sum("call_in").alias("call_in_total"),
        F.sum("call_out").alias("call_out_total"),
        F.sum("internet").alias("internet_total")
    )

    delete_path_if_exists(agg.sparkSession, f"{s3_bucket}/daily_aggregates")
    agg.write.mode("overwrite").partitionBy("date").parquet(f"{s3_bucket}/daily_aggregates")


def aggregate_hourly(df, s3_bucket):
    agg = df.groupBy("date", "hour", "cell_id").agg(
        F.sum("sms_in").alias("sms_in_total"),
        F.sum("sms_out").alias("sms_out_total"),
        F.sum("call_in").alias("call_in_total"),
        F.sum("call_out").alias("call_out_total"),
        F.sum("internet").alias("internet_total")
    )

    delete_path_if_exists(agg.sparkSession, f"{s3_bucket}/hourly_aggregates")
    agg.write.mode("overwrite").partitionBy("date", "hour").parquet(f"{s3_bucket}/hourly_aggregates")


def main(raw_path=RAW_PATH, s3_bucket=S3_BUCKET):
    spark = create_spark()

    raw = load_raw(spark, raw_path)
    transformed = transform(raw)

    delete_path_if_exists(spark, f"{s3_bucket}/raw_events")
    transformed.write.mode("overwrite").partitionBy("date").parquet(f"{s3_bucket}/raw_events")

    aggregate_daily(transformed, s3_bucket)
    aggregate_hourly(transformed, s3_bucket)

    spark.stop()


if __name__ == "__main__":
    main()