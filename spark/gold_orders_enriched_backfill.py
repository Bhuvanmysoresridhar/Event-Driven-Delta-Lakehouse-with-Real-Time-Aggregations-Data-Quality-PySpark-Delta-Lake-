# spark/gold_orders_enriched_backfill.py
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col

FACT_PATH = "data/gold/fact_orders"
DIM_PATH  = "data/gold/dim_customers"
ENR_PATH  = "data/gold/orders_enriched"

spark = (
    SparkSession.builder
    .appName("gold-orders-enriched-backfill")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Ensure target table exists (Delta, not partitioned)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS default.orders_enriched (
    order_id string,
    customer_id string,
    order_status string,
    event_ts_ms bigint,
    event_date date,
    customer_city string,
    customer_state string
  )
  USING delta
  LOCATION '{ENR_PATH}'
""")

fact = spark.read.format("delta").load(FACT_PATH)
dim  = spark.read.format("delta").load(DIM_PATH)

# join with CURRENT dim snapshot
cur = dim.where("is_current = true") \
         .select("customer_id","customer_city","customer_state")

enriched = (fact.alias("f")
    .join(cur.alias("d"), "customer_id", "left")
    .select(
        "order_id", "customer_id", "order_status",
        col("event_ts_ms"), col("event_date"),
        "customer_city", "customer_state"
    )
)

# Upsert by order_id to be idempotent
dt = DeltaTable.forName(spark, "default.orders_enriched")
(dt.alias("t")
  .merge(enriched.alias("s"), "t.order_id = s.order_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())

print("enriched row count:", dt.toDF().count())
dt.toDF().orderBy("order_id").show(truncate=False)
