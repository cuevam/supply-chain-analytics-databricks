import os
from pyspark.sql import SparkSession
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SupplyChain-Bronze") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✓ Spark session started")

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_CSV     = os.path.join(ROOT, "data/DataCoSupplyChainDataset.csv")
BRONZE_PATH = os.path.join(ROOT, "lakehouse/bronze/orders_raw")

# ── Read Raw CSV ───────────────────────────────────────────────────────────────
df_bronze = spark.read.csv(
    RAW_CSV,
    header=True,
    inferSchema=True,
    encoding="iso-8859-1"
)

print(f"✓ Raw rows loaded: {df_bronze.count():,}")
print(f"✓ Columns: {len(df_bronze.columns)}")

# ── Sanitize column names for Delta ───────────────────────────────────────────
# Delta does not allow spaces, parentheses or special chars in column names
def clean_col(name):
    name = name.strip()
    name = re.sub(r'[^\w]', '_', name)   # replace non-word chars with _
    name = re.sub(r'_+', '_', name)       # collapse multiple underscores
    name = name.strip('_')
    return name.lower()

new_cols = [clean_col(c) for c in df_bronze.columns]
df_bronze = df_bronze.toDF(*new_cols)

print("✓ Column names sanitized")

# ── Write Bronze Delta Table ───────────────────────────────────────────────────
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(BRONZE_PATH)

print(f"✓ Bronze layer written to: {BRONZE_PATH}")
print("Bronze complete. Raw data landed as Delta table.")

# ── Vacuum orphaned files ──────────────────────────────────────────────────────
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, BRONZE_PATH)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt.vacuum(0)

print("✓ Vacuum complete")

spark.stop()