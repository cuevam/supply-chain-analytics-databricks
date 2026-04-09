# =============================================================================
# 02_silver.py — Silver Layer
# Supply Chain Analytics Pipeline — Medallion Architecture
#
# Reads: lakehouse/bronze/orders_raw (Delta)
# Writes: lakehouse/silver/orders_clean (Delta, partitioned)
#
# Transformations:
#   - Cast column types
#   - Derive shipping_delay, is_late, order_year, order_month
#   - Flag fraud and canceled orders
#   - Drop PII columns + EDA-identified useless columns
#   - Deduplicate on order_item_id
# =============================================================================

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SupplyChain-Silver") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✓ Spark session started")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BRONZE_PATH = os.path.join(ROOT, "lakehouse/bronze/orders_raw")
SILVER_PATH = os.path.join(ROOT, "lakehouse/silver/orders_clean")

# ---------------------------------------------------------------------------
# 1. Read Bronze Delta table
# ---------------------------------------------------------------------------
print("Reading Bronze layer...")
df = spark.read.format("delta").load(BRONZE_PATH)
print(f"✓ Bronze row count: {df.count():,}")

# ---------------------------------------------------------------------------
# 2. Cast data types
# ---------------------------------------------------------------------------
print("Casting data types...")

df = df \
    .withColumn("order_date_dateorders",
                F.to_timestamp("order_date_dateorders", "M/d/yyyy H:mm")) \
    .withColumn("shipping_date_dateorders",
                F.to_timestamp("shipping_date_dateorders", "M/d/yyyy H:mm")) \
    .withColumn("sales",
                F.col("sales").cast(DoubleType())) \
    .withColumn("benefit_per_order",
                F.col("benefit_per_order").cast(DoubleType())) \
    .withColumn("sales_per_customer",
                F.col("sales_per_customer").cast(DoubleType())) \
    .withColumn("order_item_profit_ratio",
                F.col("order_item_profit_ratio").cast(DoubleType())) \
    .withColumn("days_for_shipping_real",
                F.col("days_for_shipping_real").cast(IntegerType())) \
    .withColumn("days_for_shipment_scheduled",
                F.col("days_for_shipment_scheduled").cast(IntegerType())) \
    .withColumn("late_delivery_risk",
                F.col("late_delivery_risk").cast(IntegerType())) \
    .withColumn("order_item_quantity",
                F.col("order_item_quantity").cast(IntegerType()))

print("✓ Data types cast")

# ---------------------------------------------------------------------------
# 3. Derive columns
# ---------------------------------------------------------------------------
print("Deriving columns...")

df = df \
    .withColumn("shipping_delay",
                F.col("days_for_shipping_real") - F.col("days_for_shipment_scheduled")) \
    .withColumn("is_late",
                F.when(F.col("shipping_delay") > 0, 1).otherwise(0)) \
    .withColumn("order_year",
                F.year("order_date_dateorders")) \
    .withColumn("order_month",
                F.month("order_date_dateorders")) \
    .withColumn("is_fraud_or_canceled",
                F.when(F.col("order_status").isin("SUSPECTED_FRAUD", "CANCELED"), 1).otherwise(0))

print("✓ Derived columns added")

# ---------------------------------------------------------------------------
# 4. Drop PII + EDA-identified useless columns
# ---------------------------------------------------------------------------
print("Dropping columns...")

COLS_TO_DROP = [
    # PII
    "customer_email",
    "customer_password",
    "customer_fname",
    "customer_lname",
    "customer_street",
    "customer_zipcode",
    # EDA findings — nulls
    "product_description",   # 100% null
    "order_zipcode",         # 86% null
]

existing = [c for c in COLS_TO_DROP if c in df.columns]
df = df.drop(*existing)
print(f"✓ Dropped columns: {existing}")

# ---------------------------------------------------------------------------
# 5. Deduplicate on order_item_id
# ---------------------------------------------------------------------------
print("Deduplicating on order_item_id...")

pre_dedup = df.count()
df = df.dropDuplicates(["order_item_id"])
post_dedup = df.count()
print(f"✓ Rows before: {pre_dedup:,}  |  After: {post_dedup:,}  |  Removed: {pre_dedup - post_dedup:,}")

# ---------------------------------------------------------------------------
# 6. Validate — null checks on key columns
# ---------------------------------------------------------------------------
print("Running null checks...")

key_cols = [
    "order_item_id",
    "order_date_dateorders",
    "days_for_shipping_real",
    "days_for_shipment_scheduled",
    "sales",
]
for col in key_cols:
    null_count = df.filter(F.col(col).isNull()).count()
    status = f"WARNING: {null_count:,} nulls" if null_count > 0 else "OK"
    print(f"  {col}: {status}")

# ---------------------------------------------------------------------------
# 7. Write Silver Delta table (partitioned)
# ---------------------------------------------------------------------------
print(f"Writing Silver layer to {SILVER_PATH}...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_year", "order_month", "order_region") \
    .save(SILVER_PATH)

print(f"✓ Silver layer written to: {SILVER_PATH}")

# ---------------------------------------------------------------------------
# 8. Vacuum orphaned files
# ---------------------------------------------------------------------------
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt = DeltaTable.forPath(spark, SILVER_PATH)
dt.vacuum(0)
print("✓ Vacuum complete")

# ---------------------------------------------------------------------------
# 9. Smoke test
# ---------------------------------------------------------------------------
print("\n--- Silver Layer Smoke Test ---")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"  Row count : {silver.count():,}")
print(f"  Columns   : {len(silver.columns)}")

silver.select(
    "order_item_id",
    "days_for_shipping_real",
    "days_for_shipment_scheduled",
    "shipping_delay",
    "is_late",
    "is_fraud_or_canceled",
    "order_year",
    "order_month",
    "order_region",
).show(10, truncate=False)

print("\n  Late delivery breakdown:")
silver.groupBy("is_late") \
    .agg(F.count("*").alias("count")) \
    .withColumn("pct", F.round(F.col("count") / silver.count() * 100, 2)) \
    .show()

print("\n  Fraud/canceled breakdown:")
silver.groupBy("is_fraud_or_canceled") \
    .agg(F.count("*").alias("count")) \
    .withColumn("pct", F.round(F.col("count") / silver.count() * 100, 2)) \
    .show()

spark.stop()
print("Done.")