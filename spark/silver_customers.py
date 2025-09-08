from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, coalesce, row_number, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "data/bronze"
SILVER_CUSTOMERS = "data/silver/customers"
CHECKPOINT = "data/checkpoints/silver/customers"
QUARANTINE = "data/quarantine/customers"

spark = (
    SparkSession.builder
    .appName("silver-customers")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "2")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read the Bronze Delta stream (from the beginning so we can build Silver deterministically)
bronze = (
    spark.readStream
    .format("delta")
    .option("startingVersion", 0)
    .load(BRONZE_PATH)
)

# Filter to just the customers topic
customers_raw = bronze.where("topic = 'olist.public.customers'")

def j(path: str):
    return get_json_object(col("v"), path)

# Parse Debezium envelope + carry Kafka ordering columns (nullable in older Bronze files)
parsed = customers_raw.select(
    col("kafka_ts"),
    coalesce(j("$.payload.op"), j("$.op")).alias("op"),
    coalesce(j("$.payload.ts_ms"), j("$.ts_ms")).cast("bigint").alias("ts_ms"),
    coalesce(
        j("$.payload.after.customer_id"), j("$.after.customer_id"),
        j("$.payload.before.customer_id"), j("$.before.customer_id")
    ).alias("customer_id"),
    coalesce(j("$.payload.after.customer_unique_id"), j("$.after.customer_unique_id")).alias("customer_unique_id"),
    coalesce(j("$.payload.after.customer_city"), j("$.after.customer_city")).alias("customer_city"),
    coalesce(j("$.payload.after.customer_state"), j("$.after.customer_state")).alias("customer_state"),
    col("kafka_partition").cast("int"),
    col("kafka_offset").cast("long"),
)

# Ensure target table exists (name -> location)
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

def process_batch(microdf, batch_id: int):
    # A) Validate records (allow deletes with key; enforce 2-letter state for upserts)
    valid = microdf.where(
        "(op = 'd' AND customer_id IS NOT NULL) OR " +
        "(op IN ('c','u','r') AND customer_id IS NOT NULL AND customer_state RLIKE '^[A-Z]{2}$')"
    )
    invalid = microdf.exceptAll(valid)

    # Quarantine bad rows (if any)
    if invalid.limit(1).count() > 0:
        invalid.write.mode("append").json(QUARANTINE)

    # Nothing to do this batch?
    if valid.limit(1).count() == 0:
        return

    # B) Dedupe by real stream order:
    #    1) latest ts_ms wins
    #    2) if ts_ms ties, higher (kafka_partition, kafka_offset) wins
    #       (coalesce to -1 so real offsets beat rows from older Bronze files)
    win = Window.partitionBy("customer_id").orderBy(
        col("ts_ms").desc(),
        coalesce(col("kafka_partition"), lit(-1)).desc(),
        coalesce(col("kafka_offset"), lit(-1)).desc(),
    )
    dedup = (
        valid.withColumn("rn", row_number().over(win))
             .where(col("rn") == 1)
             .drop("rn")
    )

    # C) MERGE (delete / upsert) into Silver
    dt = DeltaTable.forPath(spark, SILVER_CUSTOMERS)
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
