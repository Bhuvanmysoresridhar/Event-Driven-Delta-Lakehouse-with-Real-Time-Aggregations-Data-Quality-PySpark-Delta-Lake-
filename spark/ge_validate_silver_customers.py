import os, json, time
from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable

# Paths
SILVER_PATH = "data/silver/customers"
REPORT_ROOT = "data/quality/ge/silver_customers"

# Spark (Delta enabled)
spark = (
    SparkSession.builder
    .appName("ge-validate-silver-customers")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read current Silver table
df = spark.read.format("delta").load(SILVER_PATH)

# Great Expectations (V2 API)
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
ge_df = SparkDFDataset(df)

# Expectations:
# 1) customer_id must be present and unique
ge_df.expect_column_values_to_not_be_null("customer_id")
ge_df.expect_column_values_to_be_unique("customer_id")

# 2) customer_state should be two uppercase letters (e.g., 'SP', 'RJ')
ge_df.expect_column_values_to_match_regex("customer_state", r"^[A-Z]{2}$")

# 3) Optional: row count > 0 (table is not empty)
ge_df.expect_table_row_count_to_be_between(min_value=1)

# Run validation and write a JSON report
# Run validation and write a JSON report (GE 0.17 returns an object)
results = ge_df.validate(result_format="SUMMARY")

# Convert to JSON-serializable dict
try:
    results_payload = results.to_json_dict()   # GE 0.17+
except AttributeError:
    results_payload = results                  # Older GE returns a dict

ts = str(int(time.time()))
out_dir = os.path.join(REPORT_ROOT, ts)
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, "result.json"), "w") as f:
    json.dump(results_payload, f, indent=2)

# Print a crisp summary
total = df.count()
success = bool(results_payload.get("success", False))
n_failed = sum(1 for r in results_payload.get("results", []) if not r.get("success"))
print(f"[GE] success={success}; total_rows={total}; failed_expectations={n_failed}")
print("Report folder:", out_dir)
