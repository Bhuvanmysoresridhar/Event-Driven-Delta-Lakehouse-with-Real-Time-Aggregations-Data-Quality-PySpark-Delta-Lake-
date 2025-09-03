# spark/silver_customers.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, coalesce, row_number, when
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "data/bronze"
SILVER_CUSTOMERS = "data/silver/customers"
CHECKPOINT = "data/checkpoints/silver/customers"
QUARANTINE = "data/quarantine/customers"

spark = (
    SparkSession.builder
    .appName("silver-customers")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "2")
    .config("spark.executor.heartbeatInterval", "20s")
    .config("spark.network.timeout", "300s")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()

)
spark.sparkContext.setLogLevel("WARN")

# Stream read from Bronze Delta
# Stream read from Bronze Delta (force from earliest commit)
bronze = (spark.readStream
          .format("delta")
          .option("startingVersion", 0)
          .load(BRONZE_PATH))


# Only customers topic
customers_raw = bronze.where("topic = 'olist.public.customers'")

def j(path):
    return get_json_object(col("v"), path)

# Parse Debezium envelope (supports payload and flat)
parsed = customers_raw.select(
    col("kafka_ts"),
    coalesce(j("$.payload.op"), j("$.op")).alias("op"),
    coalesce(j("$.payload.ts_ms"), j("$.ts_ms")).cast("bigint").alias("ts_ms"),
    coalesce(j("$.payload.after.customer_id"), j("$.after.customer_id"),
             j("$.payload.before.customer_id"), j("$.before.customer_id")).alias("customer_id"),
    coalesce(j("$.payload.after.customer_unique_id"), j("$.after.customer_unique_id")).alias("customer_unique_id"),
    coalesce(j("$.payload.after.customer_city"), j("$.after.customer_city")).alias("customer_city"),
    coalesce(j("$.payload.after.customer_state"), j("$.after.customer_state")).alias("customer_state")
)

# Ensure target table exists once (named table + LOCATION)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.silver_customers (
    customer_id string,
    customer_unique_id string,
    customer_city string,
    customer_state string,
    _last_ts_ms bigint
  )
  USING delta
  LOCATION '{SILVER_CUSTOMERS}'
""")

def process_batch(microdf, batch_id):
    # Valid vs invalid (allow deletes with key)
    valid = microdf.where(
        "(op = 'd' AND customer_id IS NOT NULL) OR " +
        "(op IN ('c','u','r') AND customer_id IS NOT NULL AND customer_state RLIKE '^[A-Z]{2}$')"
    )
    invalid = microdf.exceptAll(valid)

    # Quarantine bad rows (cheap emptiness check)
    if invalid.limit(1).count() > 0:
        invalid.write.mode("append").json(QUARANTINE)

    # Nothing to do?
    if valid.limit(1).count() == 0:
        return

    # Deduplicate per key inside the micro-batch:
    # - keep latest ts_ms
    # - if tie on ts_ms, prefer delete (op='d') over others
    valid = valid.withColumn("op_rank", when(col("op") == "d", 0).otherwise(1))
    win = Window.partitionBy("customer_id").orderBy(col("ts_ms").desc(), col("op_rank").asc())
    dedup = (
        valid.withColumn("rn", row_number().over(win))
             .where(col("rn") == 1)
             .drop("rn", "op_rank")
    )

    # MERGE with deduped rows only
    dt = DeltaTable.forName(spark, "default.silver_customers")
    (
        dt.alias("t")
        .merge(dedup.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedDelete(condition="s.op = 'd'")
        .whenMatchedUpdate(
            condition="s.op IN ('c','u','r')",
            set={
                "customer_unique_id": "s.customer_unique_id",
                "customer_city": "s.customer_city",
                "customer_state": "s.customer_state",
                "_last_ts_ms": "s.ts_ms",
            },
        )
        .whenNotMatchedInsert(
            condition="s.op IN ('c','u','r')",
            values={
                "customer_id": "s.customer_id",
                "customer_unique_id": "s.customer_unique_id",
                "customer_city": "s.customer_city",
                "customer_state": "s.customer_state",
                "_last_ts_ms": "s.ts_ms",
            },
        )
        .execute()
    )

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
