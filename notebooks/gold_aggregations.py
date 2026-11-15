# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations & Analytics
# MAGIC
# MAGIC **Task**: Build business-ready aggregated tables from the Silver layer
# MAGIC
# MAGIC ### Gold Tables Created
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `gold_sales_summary` | Daily sales aggregated by customer segment, product category, channel, and region |
# MAGIC | `gold_customer_lifetime_value` | Customer-level CLV with RFM (Recency, Frequency, Monetary) segmentation |
# MAGIC | `gold_product_performance` | Product-level revenue, margin, and ranking metrics |
# MAGIC | `gold_monthly_trends` | Month-over-month revenue, order, and AOV trend analysis |

# COMMAND ----------

# DBTITLE 1,Import Libraries
import json

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    dense_rank,
    first,
    last,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    percent_rank,
    quarter,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
    year,
)
from pyspark.sql.window import Window

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

# DBTITLE 1,Read Silver Layer Tables
fact_transactions = spark.read.table(f"`{source_catalog}`.`{source_schema}`.fact_transactions")
dim_customer = spark.read.table(f"`{source_catalog}`.`{source_schema}`.dim_customer")
dim_product = spark.read.table(f"`{source_catalog}`.`{source_schema}`.dim_product")
dim_date = spark.read.table(f"`{source_catalog}`.`{source_schema}`.dim_date")

print(f"fact_transactions: {fact_transactions.count():,} rows")
print(f"dim_customer: {dim_customer.count():,} rows")
print(f"dim_product: {dim_product.count():,} rows")
print(f"dim_date: {dim_date.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Prepare Denormalized Fact Base
# Join fact with current dimension attributes for Gold aggregations
# Select dimension attributes, excluding business keys that already exist in fact table
current_customers = dim_customer.filter(col("is_current") == 1).select(
    col("customer_sk"),
    col("customer_name"),
    col("customer_segment"),
    col("credit_tier"),
    col("country"),
)

current_products = dim_product.filter(col("is_current") == 1).select(
    col("product_sk"),
    col("product_name"),
    col("product_category"),
    col("sub_category"),
    col("price").alias("current_price"),
    col("cost").alias("current_cost"),
    col("manufacturer"),
)

# Build the denormalized fact with the latest dimension attributes
# Join on surrogate keys; business keys (customer_id, product_id) come only from fact
enriched_fact = (
    fact_transactions.join(current_customers, on="customer_sk", how="left")
    .join(current_products, on="product_sk", how="left")
    .join(
        dim_date.select("date_key", "full_date", "day_of_week", "is_weekend"),
        col("transaction_date_key") == col("date_key"),
        "left",
    )
    .drop("date_key")
)

print(f"Enriched fact rows: {enriched_fact.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily Sales Summary

# COMMAND ----------

# DBTITLE 1,Gold: Daily Sales Summary
gold_sales_summary = (
    enriched_fact.groupBy(
        to_date(col("transaction_date")).alias("sale_date"),
        "transaction_year",
        "transaction_month",
        "transaction_quarter",
        "customer_segment",
        "product_category",
        "channel",
        "region",
    )
    .agg(
        count("transaction_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products"),
        spark_sum("quantity").alias("total_units_sold"),
        spark_round(spark_sum("transaction_amount"), 2).alias("gross_revenue"),
        spark_round(spark_sum("discount_amount"), 2).alias("total_discounts"),
        spark_round(spark_sum("net_amount"), 2).alias("net_revenue"),
        spark_round(spark_sum("tax_amount"), 2).alias("total_tax"),
        spark_round(spark_sum("total_with_tax"), 2).alias("total_revenue_with_tax"),
        spark_round(avg("transaction_amount"), 2).alias("avg_order_value"),
        spark_round(avg("discount_percentage"), 2).alias("avg_discount_pct"),
    )
    .withColumn("etl_updated_at", current_timestamp())
)

print(f"gold_sales_summary: {gold_sales_summary.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Lifetime Value & RFM Segmentation

# COMMAND ----------

# DBTITLE 1,Gold: Customer Lifetime Value
# Calculate CLV metrics per customer
clv_base = enriched_fact.groupBy("customer_id", "customer_name", "customer_segment", "credit_tier", "country").agg(
    count("transaction_id").alias("total_orders"),
    spark_sum("quantity").alias("total_units_purchased"),
    spark_round(spark_sum("net_amount"), 2).alias("total_net_revenue"),
    spark_round(spark_sum("total_with_tax"), 2).alias("total_revenue_with_tax"),
    spark_round(avg("net_amount"), 2).alias("avg_order_value"),
    spark_round(spark_sum("discount_amount"), 2).alias("total_discount_received"),
    spark_min(to_date(col("transaction_date"))).alias("first_purchase_date"),
    spark_max(to_date(col("transaction_date"))).alias("last_purchase_date"),
    countDistinct("product_id").alias("distinct_products_bought"),
    countDistinct("product_category").alias("distinct_categories"),
    countDistinct("channel").alias("channels_used"),
)

# Add RFM scores - window-based percentile ranking
clv_with_rfm = (
    clv_base.withColumn(
        "customer_tenure_days",
        datediff(col("last_purchase_date"), col("first_purchase_date")),
    )
    .withColumn(
        "days_since_last_purchase",
        datediff(current_timestamp().cast("date"), col("last_purchase_date")),
    )
    # Recency score: lower days_since = higher score
    .withColumn(
        "recency_score",
        spark_round(
            (lit(1) - percent_rank().over(Window.orderBy("days_since_last_purchase"))) * 5 + 1,
            0,
        ).cast("int"),
    )
    # Frequency score: more orders = higher score
    .withColumn(
        "frequency_score",
        spark_round(
            percent_rank().over(Window.orderBy("total_orders")) * 5 + 1,
            0,
        ).cast("int"),
    )
    # Monetary score: higher revenue = higher score
    .withColumn(
        "monetary_score",
        spark_round(
            percent_rank().over(Window.orderBy("total_net_revenue")) * 5 + 1,
            0,
        ).cast("int"),
    )
)

# Derive RFM segment label
gold_customer_ltv = clv_with_rfm.withColumn(
    "rfm_segment",
    when(
        (col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
        "Champions",
    )
    .when(
        (col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
        "Loyal Customers",
    )
    .when(
        (col("recency_score") >= 4) & (col("frequency_score") <= 2),
        "New Customers",
    )
    .when(
        (col("recency_score") <= 2) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
        "At Risk",
    )
    .when(
        (col("recency_score") <= 2) & (col("frequency_score") <= 2),
        "Lost",
    )
    .otherwise("Potential Loyalists"),
).withColumn("etl_updated_at", current_timestamp())

print(f"gold_customer_lifetime_value: {gold_customer_ltv.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Product Performance Scorecard

# COMMAND ----------

# DBTITLE 1,Gold: Product Performance
product_metrics = enriched_fact.groupBy(
    "product_id",
    "product_name",
    "product_category",
    "sub_category",
    "manufacturer",
    "current_price",
    "current_cost",
).agg(
    count("transaction_id").alias("total_orders"),
    countDistinct("customer_id").alias("unique_buyers"),
    spark_sum("quantity").alias("total_units_sold"),
    spark_round(spark_sum("net_amount"), 2).alias("total_net_revenue"),
    spark_round(spark_sum("total_with_tax"), 2).alias("total_revenue_with_tax"),
    spark_round(avg("unit_price"), 2).alias("avg_selling_price"),
    spark_round(avg("discount_percentage"), 2).alias("avg_discount_given"),
    spark_min(to_date(col("transaction_date"))).alias("first_sale_date"),
    spark_max(to_date(col("transaction_date"))).alias("last_sale_date"),
)

# Add margin and ranking
category_window = Window.partitionBy("product_category").orderBy(col("total_net_revenue").desc())
overall_window = Window.orderBy(col("total_net_revenue").desc())

gold_product_performance = (
    product_metrics.withColumn(
        "unit_margin",
        spark_round(col("current_price") - col("current_cost"), 2),
    )
    .withColumn(
        "margin_pct",
        spark_round(
            when(col("current_price") > 0, (col("current_price") - col("current_cost")) / col("current_price") * 100).otherwise(lit(0)),
            2,
        ),
    )
    .withColumn(
        "est_total_margin",
        spark_round(col("total_units_sold") * (col("current_price") - col("current_cost")), 2),
    )
    .withColumn("revenue_rank_in_category", dense_rank().over(category_window))
    .withColumn("revenue_rank_overall", dense_rank().over(overall_window))
    .withColumn(
        "revenue_contribution_pct",
        spark_round(
            col("total_net_revenue") / spark_sum("total_net_revenue").over(Window.partitionBy(lit(1))) * 100,
            2,
        ),
    )
    .withColumn("etl_updated_at", current_timestamp())
)

print(f"gold_product_performance: {gold_product_performance.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monthly Revenue Trends (MoM Analysis)

# COMMAND ----------

# DBTITLE 1,Gold: Monthly Trends
monthly_base = enriched_fact.groupBy("transaction_year", "transaction_month").agg(
    count("transaction_id").alias("order_count"),
    countDistinct("customer_id").alias("unique_customers"),
    countDistinct("product_id").alias("unique_products"),
    spark_sum("quantity").alias("total_units_sold"),
    spark_round(spark_sum("net_amount"), 2).alias("net_revenue"),
    spark_round(spark_sum("total_with_tax"), 2).alias("total_revenue"),
    spark_round(avg("net_amount"), 2).alias("avg_order_value"),
    spark_round(avg("discount_percentage"), 2).alias("avg_discount_pct"),
)

# Month-over-month window
mom_window = Window.orderBy("transaction_year", "transaction_month")

gold_monthly_trends = (
    monthly_base.withColumn(
        "prev_month_revenue",
        coalesce(
            spark_sum("net_revenue").over(mom_window.rowsBetween(-1, -1)),
            lit(0),
        ),
    )
    .withColumn(
        "revenue_mom_change",
        spark_round(col("net_revenue") - col("prev_month_revenue"), 2),
    )
    .withColumn(
        "revenue_mom_pct",
        spark_round(
            when(col("prev_month_revenue") > 0, (col("net_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100).otherwise(
                lit(0)
            ),
            2,
        ),
    )
    .withColumn(
        "prev_month_orders",
        coalesce(
            spark_sum("order_count").over(mom_window.rowsBetween(-1, -1)),
            lit(0),
        ),
    )
    .withColumn(
        "orders_mom_change",
        col("order_count") - col("prev_month_orders"),
    )
    .withColumn(
        "cumulative_revenue",
        spark_round(
            spark_sum("net_revenue").over(mom_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
            2,
        ),
    )
    .withColumn("etl_updated_at", current_timestamp())
)

print(f"gold_monthly_trends: {gold_monthly_trends.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Write Gold Layer Tables
# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")

# 1. Sales Summary - partitioned by year/month for efficient time-range queries
gold_sales_summary.write.mode("overwrite").format("delta").option("mergeSchema", "true").partitionBy(
    "transaction_year", "transaction_month"
).saveAsTable(f"`{target_catalog}`.`{target_schema}`.gold_sales_summary")

# 2. Customer Lifetime Value
gold_customer_ltv.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    f"`{target_catalog}`.`{target_schema}`.gold_customer_lifetime_value"
)

# 3. Product Performance
gold_product_performance.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    f"`{target_catalog}`.`{target_schema}`.gold_product_performance"
)

# 4. Monthly Trends
gold_monthly_trends.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    f"`{target_catalog}`.`{target_schema}`.gold_monthly_trends"
)

# COMMAND ----------

# DBTITLE 1,Log Results
sales_count = gold_sales_summary.count()
clv_count = gold_customer_ltv.count()
product_count = gold_product_performance.count()
trends_count = gold_monthly_trends.count()

print("=" * 80)
print("GOLD LAYER PROCESSING COMPLETED")
print("=" * 80)
print(f"✓ gold_sales_summary: {sales_count:,} rows")
print(f"✓ gold_customer_lifetime_value: {clv_count:,} rows")
print(f"✓ gold_product_performance: {product_count:,} rows")
print(f"✓ gold_monthly_trends: {trends_count:,} rows")

# Quick business insights
print("\n--- Quick Insights ---")
gold_customer_ltv.groupBy("rfm_segment").agg(
    count("customer_id").alias("customers"),
    spark_round(avg("total_net_revenue"), 2).alias("avg_revenue"),
).show(truncate=False)

gold_product_performance.select(
    "product_name",
    "total_net_revenue",
    "margin_pct",
    "revenue_rank_overall",
).orderBy("revenue_rank_overall").show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Return Success
dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "layer": "gold",
            "tables_created": 4,
            "gold_sales_summary_rows": sales_count,
            "gold_customer_ltv_rows": clv_count,
            "gold_product_performance_rows": product_count,
            "gold_monthly_trends_rows": trends_count,
            "run_date": run_date,
        }
    )
)
