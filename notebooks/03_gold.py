# =============================================================================
# 03_gold.py — Gold Layer
# Supply Chain Analytics Pipeline — Medallion Architecture
#
# Reads: lakehouse/silver/orders_clean (Delta)
# Writes:
#   - lakehouse/gold/orders_base         (full dataset, no aggregation)
#   - lakehouse/gold/shipping_performance (shipping KPIs)
#   - lakehouse/gold/sales_performance    (sales KPIs)
#   - lakehouse/gold/fulfillment          (fulfillment KPIs)
# =============================================================================

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SupplyChain-Gold") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✓ Spark session started")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SILVER_PATH              = os.path.join(ROOT, "lakehouse/silver/orders_clean")
GOLD_BASE                = os.path.join(ROOT, "lakehouse/gold/orders_base")
GOLD_SHIPPING            = os.path.join(ROOT, "lakehouse/gold/shipping_performance")
GOLD_SALES               = os.path.join(ROOT, "lakehouse/gold/sales_performance")
GOLD_FULFILLMENT         = os.path.join(ROOT, "lakehouse/gold/fulfillment")

# ---------------------------------------------------------------------------
# 1. Read Silver
# ---------------------------------------------------------------------------
print("Reading Silver layer...")
df = spark.read.format("delta").load(SILVER_PATH)
print(f"✓ Silver row count: {df.count():,}")

# Clean subset — excludes fraud/canceled for aggregations
df_clean = df.filter(F.col("is_fraud_or_canceled") == 0)
print(f"✓ Clean subset (excl. fraud/canceled): {df_clean.count():,}")

# ---------------------------------------------------------------------------
# 2. orders_base — full dataset, no aggregation
#    Keeps all rows including fraud/canceled for general exploration
# ---------------------------------------------------------------------------
print("\nWriting orders_base...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_year", "order_month") \
    .save(GOLD_BASE)

print(f"✓ orders_base written: {df.count():,} rows")

# ---------------------------------------------------------------------------
# 3. shipping_performance
#    - Avg shipping delay by shipping_mode
#    - Late delivery rate by shipping_mode
#    - Late delivery rate by market + order_region
# ---------------------------------------------------------------------------
print("\nBuilding shipping_performance...")

# Avg delay + late rate by shipping_mode
shipping_by_mode = df_clean \
    .groupBy("shipping_mode") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("shipping_delay"), 2).alias("avg_shipping_delay"),
        F.round(F.avg("days_for_shipping_real"), 2).alias("avg_days_actual"),
        F.round(F.avg("days_for_shipment_scheduled"), 2).alias("avg_days_scheduled"),
        F.sum("is_late").alias("late_orders"),
        F.round(F.avg("is_late") * 100, 2).alias("late_delivery_rate_pct")
    ) \
    .withColumn("grain", F.lit("shipping_mode")) \
    .withColumnRenamed("shipping_mode", "dimension_value") \
    .withColumn("market", F.lit(None).cast("string")) \
    .withColumn("order_region", F.lit(None).cast("string"))

# Late rate by market + order_region
shipping_by_region = df_clean \
    .groupBy("market", "order_region") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("shipping_delay"), 2).alias("avg_shipping_delay"),
        F.round(F.avg("days_for_shipping_real"), 2).alias("avg_days_actual"),
        F.round(F.avg("days_for_shipment_scheduled"), 2).alias("avg_days_scheduled"),
        F.sum("is_late").alias("late_orders"),
        F.round(F.avg("is_late") * 100, 2).alias("late_delivery_rate_pct")
    ) \
    .withColumn("grain", F.lit("market_region")) \
    .withColumn("dimension_value", F.concat_ws(" / ", F.col("market"), F.col("order_region")))

# Combine into single table
shipping_perf = shipping_by_mode.unionByName(shipping_by_region, allowMissingColumns=True)

shipping_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_SHIPPING)

print(f"✓ shipping_performance written: {shipping_perf.count()} rows")

# ---------------------------------------------------------------------------
# 4. sales_performance
#    - Total sales by category_name + market
#    - Avg benefit per order by customer_segment
#    - Total sales by customer_segment
# ---------------------------------------------------------------------------
print("\nBuilding sales_performance...")

sales_by_category = df_clean \
    .groupBy("category_name", "market") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.sum("sales"), 2).alias("total_sales"),
        F.round(F.avg("sales"), 2).alias("avg_order_value"),
        F.round(F.sum("benefit_per_order"), 2).alias("total_benefit"),
        F.round(F.avg("benefit_per_order"), 2).alias("avg_benefit_per_order"),
        F.round(F.avg("order_item_profit_ratio"), 4).alias("avg_profit_ratio")
    ) \
    .withColumn("customer_segment", F.lit(None).cast("string"))

sales_by_segment = df_clean \
    .groupBy("customer_segment") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(F.sum("sales"), 2).alias("total_sales"),
        F.round(F.avg("sales"), 2).alias("avg_order_value"),
        F.round(F.sum("benefit_per_order"), 2).alias("total_benefit"),
        F.round(F.avg("benefit_per_order"), 2).alias("avg_benefit_per_order"),
        F.round(F.avg("order_item_profit_ratio"), 4).alias("avg_profit_ratio")
    ) \
    .withColumn("category_name", F.lit(None).cast("string")) \
    .withColumn("market", F.lit(None).cast("string"))

sales_perf = sales_by_category.unionByName(sales_by_segment, allowMissingColumns=True)

sales_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_SALES)

print(f"✓ sales_performance written: {sales_perf.count()} rows")

# ---------------------------------------------------------------------------
# 5. fulfillment
#    - On-time vs late counts + fulfillment rate by order_year + order_month
#    - On-time vs late by order_status
# ---------------------------------------------------------------------------
print("\nBuilding fulfillment...")

fulfillment_by_month = df_clean \
    .groupBy("order_year", "order_month") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("is_late").alias("late_orders"),
        (F.count("*") - F.sum("is_late")).alias("ontime_orders"),
        F.round((1 - F.avg("is_late")) * 100, 2).alias("fulfillment_rate_pct"),
        F.round(F.avg("shipping_delay"), 2).alias("avg_shipping_delay")
    ) \
    .withColumn("grain", F.lit("month")) \
    .withColumn("order_status", F.lit(None).cast("string")) \
    .orderBy("order_year", "order_month")

fulfillment_by_status = df_clean \
    .groupBy("order_status") \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("is_late").alias("late_orders"),
        (F.count("*") - F.sum("is_late")).alias("ontime_orders"),
        F.round((1 - F.avg("is_late")) * 100, 2).alias("fulfillment_rate_pct"),
        F.round(F.avg("shipping_delay"), 2).alias("avg_shipping_delay")
    ) \
    .withColumn("grain", F.lit("order_status")) \
    .withColumn("order_year", F.lit(None).cast("int")) \
    .withColumn("order_month", F.lit(None).cast("int"))

fulfillment = fulfillment_by_month.unionByName(fulfillment_by_status, allowMissingColumns=True)

fulfillment.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_FULFILLMENT)

print(f"✓ fulfillment written: {fulfillment.count()} rows")

# ---------------------------------------------------------------------------
# 6. Vacuum all Gold tables
# ---------------------------------------------------------------------------
print("\nVacuuming Gold tables...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

for path in [GOLD_BASE, GOLD_SHIPPING, GOLD_SALES, GOLD_FULFILLMENT]:
    DeltaTable.forPath(spark, path).vacuum(0)
    print(f"✓ Vacuumed: {path}")

# ---------------------------------------------------------------------------
# 7. Smoke tests
# ---------------------------------------------------------------------------
print("\n--- Gold Layer Smoke Tests ---")

print("\n  shipping_performance sample:")
spark.read.format("delta").load(GOLD_SHIPPING) \
    .orderBy("late_delivery_rate_pct", ascending=False) \
    .show(10, truncate=False)

print("\n  sales_performance — by category + market (top 10 by total_sales):")
spark.read.format("delta").load(GOLD_SALES) \
    .filter(F.col("category_name").isNotNull()) \
    .orderBy("total_sales", ascending=False) \
    .show(10, truncate=False)

print("\n  fulfillment rate by month:")
spark.read.format("delta").load(GOLD_FULFILLMENT) \
    .filter(F.col("grain") == "month") \
    .orderBy("order_year", "order_month") \
    .show(36, truncate=False)

spark.stop()
print("\nGold layer complete.")