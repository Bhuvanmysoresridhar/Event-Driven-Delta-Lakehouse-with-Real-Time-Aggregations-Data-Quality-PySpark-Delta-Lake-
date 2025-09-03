# spark/gold_dim_customers_scd2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, coalesce, lit, row_number, when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "data/bronze"
DIM_PATH = "data/gold/dim_customers"
CHECKPOINT = "data/checkpoints/gold/dim_customers"

spark = (
    SparkSession.builder
    .appName("gold-dim-customers-scd2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Ensure target table exists (named + LOCATION)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.dim_customers (
    customer_id string,
    customer_unique_id string,
    customer_city string,
    customer_state string,
    effective_start_ts_ms bigint,
    effective_end_ts_ms bigint,
    is_current boolean
  )
  USING delta
  LOCATION '{DIM_PATH}'
""")

# Stream source = Bronze Delta (append-only)
bronze = spark.readStream.format("delta").load(BRONZE_PATH)
customers_raw = bronze.where("topic = 'olist.public.customers'")

def j(path):
    return get_json_object(col("v"), path)

# Parse Debezium envelope (payload + flat supported)
parsed = customers_raw.select(
    coalesce(j("$.payload.op"), j("$.op")).alias("op"),
    coalesce(j("$.payload.ts_ms"), j("$.ts_ms")).cast("bigint").alias("ts_ms"),
    coalesce(j("$.payload.after.customer_id"), j("$.after.customer_id"),
             coalesce(j("$.payload.before.customer_id"), j("$.before.customer_id"))).alias("customer_id"),
    coalesce(j("$.payload.after.customer_unique_id"), j("$.after.customer_unique_id")).alias("customer_unique_id"),
    coalesce(j("$.payload.after.customer_city"), j("$.after.customer_city")).alias("customer_city"),
    coalesce(j("$.payload.after.customer_state"), j("$.after.customer_state")).alias("customer_state")
)

def process_batch(microdf, batch_id):
    # Keep only rows with a key
    microdf = microdf.where("customer_id IS NOT NULL")

    # Filter: allow deletes by key; for c/u/r require two-letter state
    valid = microdf.where(
        "(op = 'd' AND customer_id IS NOT NULL) OR " +
        "(op IN ('c','u','r') AND customer_id IS NOT NULL AND customer_state RLIKE '^[A-Z]{2}$')"
    )
    invalid = microdf.exceptAll(valid)

    # Quarantine invalid rows for Gold
    if invalid.limit(1).count() > 0:
        invalid.write.mode("append").json("data/quarantine/gold_dim_customers")

    # Nothing to do?
    if valid.limit(1).count() == 0:
        return

    # Deduplicate per customer_id within this micro-batch:
    # - keep latest ts_ms
    # - if tie, prefer delete (so deletes win)
    valid = valid.withColumn("op_rank", when(col("op") == "d", 0).otherwise(1))
    win = Window.partitionBy("customer_id").orderBy(col("ts_ms").desc(), col("op_rank").asc())
    s = (valid
         .withColumn("rn", row_number().over(win))
         .where(col("rn") == 1)
         .drop("rn", "op_rank"))

    # Current dim snapshot for comparisons
    dt = DeltaTable.forName(spark, "default.dim_customers")
    cur = (dt.toDF()
           .where("is_current = true")
           .selectExpr(
               "customer_id as t_customer_id",
               "customer_unique_id as t_customer_unique_id",
               "customer_city as t_customer_city",
               "customer_state as t_customer_state"
           ))

    joined = s.join(cur, s.customer_id == col("t_customer_id"), "left")

    # Attribute change detection (null-safe)
    diff = (
        coalesce(col("customer_unique_id"), lit("")) != coalesce(col("t_customer_unique_id"), lit(""))
    ) | (
        coalesce(col("customer_city"), lit("")) != coalesce(col("t_customer_city"), lit(""))
    ) | (
        coalesce(col("customer_state"), lit("")) != coalesce(col("t_customer_state"), lit(""))
    )

    # 1) CLOSE records: deletes OR changed attributes where a current exists
    to_close = joined.where(
        (col("op") == lit("d")) |
        ((col("op").isin("c","u","r")) & col("t_customer_id").isNotNull() & diff)
    ).select(s.customer_id.alias("customer_id"), s.ts_ms.alias("ts_ms"))

    if to_close.limit(1).count() > 0:
        (dt.alias("t")
           .merge(to_close.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
           .whenMatchedUpdate(set={
               "effective_end_ts_ms": "s.ts_ms",
               "is_current": "false"
           })
           .execute())

    # 2) INSERT new current versions: new keys or changed attributes
    to_insert = joined.where(
        (col("op").isin("c","u","r")) &
        (col("t_customer_id").isNull() | diff)
    ).select(
        s.customer_id,
        s.customer_unique_id,
        s.customer_city,
        s.customer_state,
        s.ts_ms.alias("effective_start_ts_ms")
    )

    if to_insert.limit(1).count() > 0:
       rows = (to_insert
            .withColumn("effective_end_ts_ms", lit(None).cast("bigint"))
            .withColumn("is_current", lit(True)))
    tgt = DeltaTable.forName(spark, "default.dim_customers")
    (
        tgt.alias("t")
        .merge(
            rows.alias("s"),
            "t.customer_id = s.customer_id AND t.effective_start_ts_ms = s.effective_start_ts_ms"
        )
        .whenNotMatchedInsert(values={
            "customer_id": "s.customer_id",
            "customer_unique_id": "s.customer_unique_id",
            "customer_city": "s.customer_city",
            "customer_state": "s.customer_state",
            "effective_start_ts_ms": "s.effective_start_ts_ms",
            "effective_end_ts_ms": "s.effective_end_ts_ms",
            "is_current": "s.is_current",
        })
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
