# spark/reconcile_fact_vs_agg.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FACT_PATH = os.path.abspath("data/gold/fact_orders")
AGG_PATH  = os.path.abspath("data/gold/agg_orders_daily")
WAREHOUSE = os.path.abspath("spark-warehouse")

spark = (
    SparkSession.builder
      .appName("reconcile-fact-vs-agg")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
      .config("spark.sql.warehouse.dir", WAREHOUSE)
      .getOrCreate()
)

# Always read by PATH to avoid missing-table-name errors
fact_df = spark.read.format("delta").load(FACT_PATH)
agg_df  = spark.read.format("delta").load(AGG_PATH)

fact_g = (
    fact_df.groupBy("event_date","order_status")
           .agg(F.count(F.lit(1)).alias("cnt_fact"))
)

agg_g = (
    agg_df.select("event_date","order_status","order_cnt")
          .withColumnRenamed("order_cnt","cnt_agg")
)

print(f"fact rows (path): {fact_df.count()}  |  agg rows (path): {agg_df.count()}")
print("Any diff fact vs agg?")
(
    fact_g.join(agg_g, ["event_date","order_status"], "full_outer")
          .select(
              "event_date","order_status",
              "cnt_fact","cnt_agg"
          )
          .where(
              F.coalesce(F.col("cnt_fact"), F.lit(0))
              != F.coalesce(F.col("cnt_agg"), F.lit(0))
          )
          .orderBy("event_date","order_status")
          .show(truncate=False)
)

# Optional: show the current snapshots so you can eyeball them
print("\n-- fact snapshot (path) --")
(
    fact_df.groupBy("event_date","order_status")
           .count()
           .orderBy("event_date","order_status")
           .show(truncate=False)
)

print("\n-- agg snapshot (path) --")
(
    agg_df.orderBy("event_date","order_status")
          .show(truncate=False)
)

