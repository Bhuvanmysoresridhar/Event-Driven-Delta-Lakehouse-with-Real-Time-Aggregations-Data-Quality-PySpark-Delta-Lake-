# spark/smoke_test_end_to_end.py
import os
from pyspark.sql import SparkSession

FACT_PATH = os.path.abspath("data/gold/fact_orders")
AGG_PATH  = os.path.abspath("data/gold/agg_orders_daily")

spark = (
    SparkSession.builder
      .appName("smoke-test-end-to-end")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
      .config("spark.sql.warehouse.dir","spark-warehouse")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Always register names to the actual Delta tables on disk (no partition/schema declared)
spark.sql("DROP TABLE IF EXISTS default.fact_orders")
spark.sql(f"CREATE TABLE default.fact_orders USING delta LOCATION '{FACT_PATH}'")
spark.sql("ALTER TABLE default.fact_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

spark.sql("DROP TABLE IF EXISTS default.agg_orders_daily")
spark.sql(f"CREATE TABLE default.agg_orders_daily USING delta LOCATION '{AGG_PATH}'")
spark.sql("REFRESH TABLE default.agg_orders_daily")

print("\n-- SHOW TABLES (default) --")
spark.sql("SHOW TABLES IN default LIKE 'fact_orders'").show(truncate=False)
spark.sql("SHOW TABLES IN default LIKE 'agg_orders_daily'").show(truncate=False)

# Write a few changes to fact
print("\n-- Writing smoke rows to fact_orders --")
spark.sql("""
  INSERT INTO default.fact_orders
  SELECT 'o_smoke_demo','c_001','created',
         CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT), CURRENT_DATE()
""")

spark.sql("""
  UPDATE default.fact_orders
  SET order_status = 'approved'
  WHERE order_id = 'o_smoke_demo'
""")

spark.sql("""
  INSERT INTO default.fact_orders
  SELECT 'o_smoke_demo_2','c_001','approved',
         CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT), CURRENT_DATE()
""")

print("\nWrote smoke-test changes.")

# Snapshots
print("\n-- fact snapshot (by name) --")
spark.table("default.fact_orders") \
     .groupBy("event_date","order_status").count() \
     .orderBy("event_date","order_status") \
     .show(truncate=False)

print("\n-- agg snapshot (by name) --")
spark.table("default.agg_orders_daily") \
     .orderBy("event_date","order_status") \
     .show(truncate=False)

print("\n-- agg snapshot (by path, truth) --")
spark.read.format("delta").load(AGG_PATH) \
     .orderBy("event_date","order_status") \
     .show(truncate=False)

print("\nIf agg by name lags, ensure the stream is running with the correct CHECKPOINT and AGG_PATH.")
