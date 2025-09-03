# spark/ge_validate_agg_orders_daily.py
import os, json, time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from great_expectations.execution_engine.sparkdf_execution_engine import SparkDFExecutionEngine
from great_expectations.validator.validator import Validator
from great_expectations.core.batch import Batch, BatchDefinition, BatchMarkers
from great_expectations.core.id_dict import IDDict

AGG_PATH = os.path.abspath("data/gold/agg_orders_daily")
OUT_DIR  = os.path.abspath(f"data/quality/ge/agg_orders_daily/{int(time.time())}")
os.makedirs(OUT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load(AGG_PATH)

# --- GE v1.x: proper Batch with required ge_load_time marker ---
engine = SparkDFExecutionEngine()

ge_load_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
batch = Batch(
    data=df,
    batch_definition=BatchDefinition(
        datasource_name="inline",
        data_connector_name="runtime",
        data_asset_name="agg_orders_daily",
        batch_identifiers=IDDict({"default_identifier_name": "default"})
    ),
    batch_markers=BatchMarkers({"ge_load_time": ge_load_time})
)

validator = Validator(execution_engine=engine, batches=[batch])

# --- Expectations ---
validator.expect_table_row_count_to_be_between(min_value=0)

for col in ["event_date", "order_status", "order_cnt"]:
    validator.expect_column_to_exist(col)
validator.expect_column_values_to_not_be_null("event_date")
validator.expect_column_values_to_not_be_null("order_status")
validator.expect_column_values_to_be_between("order_cnt", min_value=0)

validator.expect_column_values_to_be_in_set(
    "order_status",
    ["created","approved","delivered","canceled","invoiced","shipped","processing"]
)

# Optional uniqueness at the aggregate grain
try:
    validator.expect_compound_columns_to_be_unique(["event_date","order_status"])
except Exception:
    pass

res = validator.validate()

report = {
    "success": res["success"],
    "statistics": res.get("statistics", {}),
    "results": [
        {
            "expectation": r["expectation_config"]["expectation_type"],
            "success": r["success"]
        } for r in res.get("results", [])
    ]
}

with open(os.path.join(OUT_DIR, "result.json"), "w") as f:
    json.dump(report, f, indent=2)

print(f"[GE agg] success={report['success']}; "
      f"total={report.get('statistics',{}).get('evaluated_expectations','?')}")
print("Report folder:", OUT_DIR)

