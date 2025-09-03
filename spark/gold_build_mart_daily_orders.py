from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from delta.tables import DeltaTable

FACT_PATH = "data/gold/fact_orders"
DIM_PATH  = "data/gold/dim_customers"
MART_PATH = "data/gold/mart_daily_orders"

spark = (
    SparkSession.builder
    .appName("gold-build-mart-daily-orders")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read facts (batch) and current dimension snapshot for state
facts = spark.read.format("delta").load(FACT_PATH)
dim   = spark.read.format("delta").load(DIM_PATH).where("is_current = true") \
        .select("customer_id","customer_state")

# Join and aggregate
joined = facts.join(dim, "customer_id", "left")
agg = (joined.groupBy("event_date","customer_state","order_status")
             .agg(count("*").alias("order_count")))

# Initialize the mart folder/table if needed
if not DeltaTable.isDeltaTable(spark, MART_PATH):
    agg.limit(0).write.format("delta").mode("overwrite").save(MART_PATH)

# Register/overwrite the table (full refresh for simplicity)
spark.sql("DROP TABLE IF EXISTS default.mart_daily_orders")
(agg.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date")
    .option("overwriteSchema","true")
    .save(MART_PATH))

spark.sql(f"""
  CREATE TABLE default.mart_daily_orders
  USING delta
  LOCATION '{MART_PATH}'
""")

print("mart_daily_orders built at", MART_PATH)
