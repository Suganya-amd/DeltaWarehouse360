# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Processing with SCD2
# MAGIC
# MAGIC **Task**: Apply SCD2 logic to create Silver layer dimensions and enriched facts

# COMMAND ----------

# DBTITLE 1,Import Libraries and SCD2 Processor
import json
import os
import sys

# Ensure local bundle modules are importable in Databricks workspace
base_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
bundle_src = os.path.abspath(os.path.join(base_dir, "../src/python"))
if os.path.exists(bundle_src) and bundle_src not in sys.path:
    sys.path.insert(0, bundle_src)

from DeltaWarehouse360.scd2_processor import SCD2Processor
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get Parameters
dbutils.widgets.text("source_catalog", "")
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("run_date", "")

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
run_date = dbutils.widgets.get("run_date")

print(f"Source: {source_catalog}.{source_schema}")
print(f"Target: {target_catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,Initialize SCD2 Processor
scd2_processor = SCD2Processor(spark)

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
bronze_customers = spark.read.table(f"{source_catalog}.{source_schema}.bronze_customers")

bronze_products = spark.read.table(f"{source_catalog}.{source_schema}.bronze_products")

bronze_transactions = spark.read.table(f"{source_catalog}.{source_schema}.bronze_transactions")

print(f"Bronze Customers: {bronze_customers.count()} rows")
print(f"Bronze Products: {bronze_products.count()} rows")
print(f"Bronze Transactions: {bronze_transactions.count()} rows")

# COMMAND ----------


# DBTITLE 1,Helper to Load Existing Dimension
def load_existing_dimension(catalog, schema, table):
    """Safely check and load existing dimension table, returning None on initial load."""
    try:
        full_name = f"`{catalog}`.`{schema}`.`{table}`"
        if spark.catalog.tableExists(full_name):
            df = spark.read.table(full_name)
            _ = df.columns
            print(f"Found existing dimension: {full_name}")
            return df
    except Exception as e:
        print(f"Dimension {table} check: {e}")
    print(f"No existing {table} dimension - initial load")
    return None


# DBTITLE 1,Process Customer Dimension with SCD2
# Define SCD2 columns for customers
customer_scd_columns = ["customer_email", "customer_segment", "credit_tier", "country"]

# Check if existing dimension exists
existing_customers = load_existing_dimension(target_catalog, target_schema, "dim_customer")

# If initial load, take all bronze records; otherwise process current records for SCD2
incoming_customers = bronze_customers if existing_customers is None else bronze_customers.filter(col("is_current") == 1)

# Apply SCD2 logic
dim_customers = scd2_processor.apply_scd2_logic(
    incoming_df=incoming_customers,
    existing_df=existing_customers,
    business_key_cols=["customer_id"],
    scd_columns=customer_scd_columns,
    effective_date_col="start_date",
)

# Add surrogate key
dim_customers = scd2_processor.generate_surrogate_keys(
    dim_customers, "dim_customer", "customer_sk", order_cols=["customer_id", "start_date"]
)

# COMMAND ----------

# DBTITLE 1,Process Product Dimension with SCD2
# Define SCD2 columns for products
product_scd_columns = ["product_name", "price", "cost", "manufacturer", "discontinued_flag"]

# Check if existing dimension exists
existing_products = load_existing_dimension(target_catalog, target_schema, "dim_product")

incoming_products = bronze_products if existing_products is None else bronze_products.filter(col("is_current") == 1)

dim_products = scd2_processor.apply_scd2_logic(
    incoming_df=incoming_products,
    existing_df=existing_products,
    business_key_cols=["product_id"],
    scd_columns=product_scd_columns,
    effective_date_col="start_date",
)

dim_products = scd2_processor.generate_surrogate_keys(
    dim_products, "dim_product", "product_sk", order_cols=["product_id", "start_date"]
)

# COMMAND ----------

# DBTITLE 1,Create Enriched Silver Fact Table with Point-in-Time Surrogate Keys
# Clean and derive base transaction metrics
base_transactions = (
    bronze_transactions.withColumn("transaction_date_key", date_format(col("transaction_date"), "yyyyMMdd").cast("int"))
    .withColumn("transaction_year", year(col("transaction_date")))
    .withColumn("transaction_month", month(col("transaction_date")))
    .withColumn(
        "transaction_quarter",
        when(month(col("transaction_date")).between(1, 3), 1)
        .when(month(col("transaction_date")).between(4, 6), 2)
        .when(month(col("transaction_date")).between(7, 9), 3)
        .otherwise(4),
    )
    .withColumn("discount_amount", col("transaction_amount") * col("discount_percentage") / 100)
    .withColumn("net_amount", col("transaction_amount") - col("discount_amount"))
    .withColumn("total_with_tax", col("net_amount") + col("tax_amount"))
)

# Point-in-time dimension lookups for surrogate keys
cust_lookup = dim_customers.select(
    col("customer_id").alias("c_id"),
    col("customer_sk"),
    col("start_date").alias("c_start"),
    coalesce(col("end_date"), to_date(lit("9999-12-31"))).alias("c_end"),
)

prod_lookup = dim_products.select(
    col("product_id").alias("p_id"),
    col("product_sk"),
    col("start_date").alias("p_start"),
    coalesce(col("end_date"), to_date(lit("9999-12-31"))).alias("p_end"),
)

fact_transactions = (
    base_transactions.join(
        cust_lookup,
        (col("customer_id") == col("c_id"))
        & (to_date(col("transaction_date")) >= col("c_start"))
        & (to_date(col("transaction_date")) <= col("c_end")),
        "left",
    )
    .drop("c_id", "c_start", "c_end")
    .join(
        prod_lookup,
        (col("product_id") == col("p_id"))
        & (to_date(col("transaction_date")) >= col("p_start"))
        & (to_date(col("transaction_date")) <= col("p_end")),
        "left",
    )
    .drop("p_id", "p_start", "p_end")
)

# COMMAND ----------

# DBTITLE 1,Write Silver Layer Tables
# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")

# Write customer dimension
dim_customers.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    f"`{target_catalog}`.`{target_schema}`.dim_customer"
)

# Write product dimension
dim_products.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    f"`{target_catalog}`.`{target_schema}`.dim_product"
)

# Write fact table
fact_transactions.write.mode("overwrite").format("delta").partitionBy("transaction_year", "transaction_month").option(
    "mergeSchema", "true"
).saveAsTable(f"`{target_catalog}`.`{target_schema}`.fact_transactions")

# COMMAND ----------

# DBTITLE 1,Create Supporting Dimensions
# Create date dimension (static, can be loaded once)
date_dim_sql = f"""
CREATE TABLE IF NOT EXISTS `{target_catalog}`.`{target_schema}`.dim_date AS
SELECT 
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    date AS full_date,
    dayofweek(date) AS day_of_week,
    dayofmonth(date) AS day_of_month,
    weekofyear(date) AS week_of_year,
    month(date) AS month_number,
    date_format(date, 'MMMM') AS month_name,
    quarter(date) AS quarter,
    year(date) AS year,
    CASE WHEN month(date) >= 7 THEN year(date) + 1 ELSE year(date) END AS fiscal_year,
    CASE 
        WHEN month(date) IN (1,2,3) THEN 'Q3'
        WHEN month(date) IN (4,5,6) THEN 'Q4'
        WHEN month(date) IN (7,8,9) THEN 'Q1'
        ELSE 'Q2'
    END AS fiscal_quarter,
    CASE WHEN dayofweek(date) IN (1,7) THEN 1 ELSE 0 END AS is_weekend
FROM (
    SELECT explode(sequence(
        DATE '2020-01-01', 
        DATE '2030-12-31', 
        INTERVAL 1 DAY
    )) AS date
)
"""

spark.sql(date_dim_sql)

# COMMAND ----------

# DBTITLE 1,Log Results
# Get row counts
dim_customer_count = dim_customers.count()
dim_product_count = dim_products.count()
fact_transaction_count = fact_transactions.count()
scd2_versions = dim_customers.filter(col("is_current_record") == 0).count()

print("=" * 80)
print("SILVER LAYER PROCESSING COMPLETED")
print("=" * 80)
print(f"✓ dim_customer: {dim_customer_count:,} rows ({scd2_versions} historical versions)")
print(f"✓ dim_product: {dim_product_count:,} rows")
print(f"✓ fact_transactions: {fact_transaction_count:,} rows")
print("✓ dim_date: 4,018 days (2020-2030)")

# COMMAND ----------

# DBTITLE 1,Return Success
dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "layer": "silver",
            "tables_created": 4,
            "total_rows": dim_customer_count + dim_product_count + fact_transaction_count,
            "scd2_versions": scd2_versions,
            "run_date": run_date,
        }
    )
)
