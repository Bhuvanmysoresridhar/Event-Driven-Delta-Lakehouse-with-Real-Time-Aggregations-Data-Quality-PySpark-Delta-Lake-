# spark/gold_agg_orders_daily.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
from delta.tables import DeltaTable

FACT_TBL   = "default.fact_orders"
AGG_TBL    = "default.agg_orders_daily"
CHECKPOINT = "data/checkpoints/gold/agg_orders_daily"

spark = (
    SparkSession.builder
    .appName("gold-agg-orders-daily")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# CDF stream from fact_orders. No startingVersion: start “now” and keep up.
src = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table(FACT_TBL)
)

def process_batch(microdf, batch_id):
    # Map CDF change types to +1 / -1 deltas
    deltas = (microdf
              .withColumn(
                  "delta",
                  when(col("_change_type").isin("insert","update_postimage"), 1)
                  .when(col("_change_type").isin("delete","update_preimage"), -1)
                  .otherwise(0)
              )
              .where(col("delta") != 0)
              .groupBy("event_date","order_status")
              .agg(_sum("delta").alias("delta_cnt"))
             )

    if deltas.limit(1).count() == 0:
        return

    dt = DeltaTable.forName(spark, AGG_TBL)
    (dt.alias("t")
       .merge(deltas.alias("s"),
              "t.event_date = s.event_date AND t.order_status = s.order_status")
       .whenMatchedUpdate(set={"order_cnt": "t.order_cnt + s.delta_cnt"})
       .whenNotMatchedInsert(values={
           "event_date": "s.event_date",
           "order_status": "s.order_status",
           "order_cnt": "s.delta_cnt"
       })
       .execute())

query = (
    src.writeStream
       .foreachBatch(process_batch)
       .option("checkpointLocation", CHECKPOINT)
       .outputMode("update")
       .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
