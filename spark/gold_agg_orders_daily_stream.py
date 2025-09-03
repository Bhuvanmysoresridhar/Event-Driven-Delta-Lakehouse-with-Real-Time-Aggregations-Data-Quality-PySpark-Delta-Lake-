# spark/gold_agg_orders_daily_stream.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as Fsum, lit
from delta.tables import DeltaTable

# ---- Paths (use absolute to avoid metastore confusion) ----
FACT_PATH = os.path.abspath("data/gold/fact_orders")
AGG_PATH  = os.path.abspath("data/gold/agg_orders_daily")
CHECKPOINT = "data/checkpoints/gold/agg_orders_daily"  # keep relative is fine

# ---- Spark session ----
spark = (
    SparkSession.builder
    .appName("gold-agg-orders-daily-stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---- Figure out where to start CDF (next commit after latest) ----
last_version = (
    spark.sql(f"DESCRIBE HISTORY delta.`{FACT_PATH}`")
         .selectExpr("max(version) as v")
         .collect()[0].v
)
start_version = 0 if last_version is None else int(last_version) + 1
print(f"[agg_stream] starting from version {start_version} (process only commits > {last_version})")

# ---- Read fact CDF by PATH ----
src = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", start_version)
    .load(FACT_PATH)
)

def process_batch(df, batch_id):
    # Skip empty micro-batches quickly
    if df.limit(1).count() == 0:
        return

    # Map change types to +/-1 deltas
    deltas = (
        df.select(
            "event_date",
            "order_status",
            when(col("_change_type").isin("insert", "update_postimage"), lit(1))
            .when(col("_change_type").isin("delete", "update_preimage"), lit(-1))
            .otherwise(lit(0)).alias("d")
        )
        .groupBy("event_date", "order_status")
        .agg(Fsum("d").alias("delta_cnt"))
        .where(col("delta_cnt") != 0)
    )

    if deltas.limit(1).count() == 0:
        return

    # Merge into target AGG by PATH
    tgt = DeltaTable.forPath(spark, AGG_PATH)

    (
        tgt.alias("t")
        .merge(
            deltas.alias("s"),
            "t.event_date = s.event_date AND t.order_status = s.order_status",
        )
        .whenMatchedUpdate(set={"order_cnt": "t.order_cnt + s.delta_cnt"})
        .whenNotMatchedInsert(values={
            "event_date": "s.event_date",
            "order_status": "s.order_status",
            "order_cnt": "s.delta_cnt",
        })
        .execute()
    )

# Kick off the stream
q = (
    src.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

q.awaitTermination()


