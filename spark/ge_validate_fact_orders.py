# spark/ge_validate_fact_orders.py
import os, time, json, sys
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset  # legacy, simple & effective

FACT_PATH = "data/gold/fact_orders"
REPORT_ROOT = "data/quality/ge/fact_orders"

# Spark with Delta
spark = (
    SparkSession.builder
    .appName("ge-validate-fact-orders")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---- Load table (fail fast if path missing)
try:
    df = spark.read.format("delta").load(FACT_PATH)
except Exception as e:
    print(f"[GE] Could not load Delta at {FACT_PATH}: {e}")
    sys.exit(2)

row_count = df.count()
print(f"[GE] fact_orders rows: {row_count}")

# ---- Wrap with GE Spark dataset
gx = SparkDFDataset(df)

# ---- Expectations
# 0) Table must not be empty
gx.expect_table_row_count_to_be_between(min_value=1)

# 1) Keys present & valid
gx.expect_column_values_to_not_be_null("order_id")
gx.expect_column_values_to_not_be_null("customer_id")
gx.expect_column_values_to_be_unique("order_id")

# 2) Status constrained to known set (tweak if you add new statuses)
allowed_status = [
    "created","approved","invoiced","shipped","delivered",
    "canceled","processing","unavailable","in_review","returned"
]
gx.expect_column_values_to_not_be_null("order_status")
gx.expect_column_values_to_be_in_set("order_status", allowed_status)

# 3) Partition/date present
gx.expect_column_values_to_not_be_null("event_date")
gx.expect_column_unique_value_count_to_be_between("event_date", min_value=1)

# 4) event_ts_ms looks like a positive epoch millis
gx.expect_column_values_to_not_be_null("event_ts_ms")
gx.expect_column_values_to_be_between("event_ts_ms", min_value=0)

# ---- Validate & write JSON report
results = gx.validate()
result_dict = results.to_json_dict() if hasattr(results, "to_json_dict") else results

ts = str(int(time.time()))
out_dir = os.path.join(REPORT_ROOT, ts)
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "result.json")
with open(out_path, "w") as f:
    json.dump(result_dict, f, indent=2)

success = result_dict["success"] if isinstance(result_dict, dict) else False
total_expectations = len(result_dict.get("results", [])) if isinstance(result_dict, dict) else 0
failed = sum(1 for r in result_dict.get("results", []) if not r.get("success", False)) if isinstance(result_dict, dict) else "?"

print(f"[GE] success={success}; total_expectations={total_expectations}; failed={failed}")
print("Report folder:", out_dir)

# exit non-zero if checks failed
sys.exit(0 if success else 1)
