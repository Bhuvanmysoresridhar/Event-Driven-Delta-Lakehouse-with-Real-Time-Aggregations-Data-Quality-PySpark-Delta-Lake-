# spark/gold_fact_orders.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, coalesce, to_date, from_unixtime, when, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "data/bronze"
FACT_PATH = "data/gold/fact_orders"
CHECKPOINT = "data/checkpoints/gold/fact_orders"

spark = (
    SparkSession.builder
    .appName("gold-fact-orders")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Ensure target table exists (partitioned by calendar day)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.fact_orders (
    order_id string,
    customer_id string,
    order_status string,
    event_ts_ms bigint,
    event_date date
  )
  USING delta
  PARTITIONED BY (event_date)
  LOCATION '{FACT_PATH}'
""")

# Read CDC from Bronze
bronze = spark.readStream.format("delta").load(BRONZE_PATH)
orders_raw = bronze.where("topic = 'olist.public.orders'")

def j(path): return get_json_object(col("v"), path)

# Parse Debezium envelope (payload & flat)
parsed = orders_raw.select(
    coalesce(j("$.payload.op"), j("$.op")).alias("op"),
    coalesce(j("$.payload.ts_ms"), j("$.ts_ms")).cast("bigint").alias("ts_ms"),
    coalesce(j("$.payload.after.order_id"), j("$.after.order_id"),
             j("$.payload.before.order_id"), j("$.before.order_id")).alias("order_id"),
    coalesce(j("$.payload.after.customer_id"), j("$.after.customer_id"),
             j("$.payload.before.customer_id"), j("$.before.customer_id")).alias("customer_id"),
    coalesce(j("$.payload.after.order_status"), j("$.after.order_status")).alias("order_status")
).withColumn("event_date", to_date(from_unixtime(col("ts_ms")/1000.0)))

def process_batch(microdf, batch_id):
    # Valid if: delete with key, or upsert with both keys present
    valid = microdf.where(
        "(op = 'd' AND order_id IS NOT NULL) OR " +
        "(op IN ('c','u','r') AND order_id IS NOT NULL AND customer_id IS NOT NULL)"
    )
    if valid.limit(1).count() == 0:
        return

    # Dedup per order_id (latest ts_ms wins; prefer delete on ties)
    valid = valid.withColumn("op_rank", when(col("op") == "d", 0).otherwise(1))
    win = Window.partitionBy("order_id").orderBy(col("ts_ms").desc(), col("op_rank").asc())
    s = (valid.withColumn("rn", row_number().over(win))
              .where(col("rn") == 1).drop("rn", "op_rank"))

    # Merge into target
    dt = DeltaTable.forPath(spark, FACT_PATH)
    (dt.alias("t")
      .merge(s.alias("s"), "t.order_id = s.order_id")
      .whenMatchedDelete(condition="s.op = 'd'")
      .whenMatchedUpdate(
          condition="s.op IN ('c','u','r')",
          set={
              "customer_id": "s.customer_id",
              "order_status": "s.order_status",
              "event_ts_ms": "s.ts_ms",
              "event_date": "s.event_date",
          }
      )
      .whenNotMatchedInsert(
          condition="s.op IN ('c','u','r')",
          values={
              "order_id": "s.order_id",
              "customer_id": "s.customer_id",
              "order_status": "s.order_status",
              "event_ts_ms": "s.ts_ms",
              "event_date": "s.event_date",
          }
      )
      .execute())

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
