# spark/bronze_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP = "localhost:9093"
TOPICS = ",".join([
    "olist.public.customers",
    "olist.public.products",
    "olist.public.orders",
    "olist.public.order_items",
])

BRONZE_PATH = "data/bronze"
CHECKPOINT_PATH = "data/checkpoints/bronze"

spark = (
    SparkSession.builder
    .appName("bronze-stream")
    # Enable Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Pull BOTH Delta and Kafka packages
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribePattern", "olist.public.*")  # pick up customers, orders, order_items, products, etc.
    .option("startingOffsets", "earliest")
    .load()
)

df = raw.select(
    col("topic"),
    col("key").cast("string").alias("k"),
    col("value").cast("string").alias("v"),
    col("timestamp").alias("kafka_ts"),
)

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("path", BRONZE_PATH)
    .trigger(processingTime="15 seconds")
    .start()
)

query.awaitTermination()
