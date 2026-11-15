# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC
# MAGIC **Task**: Read CSV files from volume and load to Bronze Delta tables

# COMMAND ----------

# DBTITLE 1,Import Libraries
import builtins
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Get Parameters
dbutils.widgets.text("source_volume", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("run_date", "")

source_volume = dbutils.widgets.get("source_volume")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")

print(f"Source Volume: {source_volume}")
print(f"Target Catalog: {catalog}")
print(f"Target Schema: {schema}")
print(f"Run Date: {run_date}")

# COMMAND ----------

# DBTITLE 1,Define CSV Schemas
customer_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("credit_tier", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_current", IntegerType(), True),
        StructField("last_updated", TimestampType(), True),
    ]
)

product_schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("cost", DecimalType(10, 2), True),
        StructField("manufacturer", StringType(), True),
        StructField("discontinued_flag", IntegerType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_current", IntegerType(), True),
        StructField("last_updated", TimestampType(), True),
    ]
)

transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("transaction_amount", DecimalType(10, 2), True),
        StructField("discount_percentage", DecimalType(5, 2), True),
        StructField("tax_amount", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("payment_method", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sales_person_id", StringType(), True),
    ]
)

# COMMAND ----------


# DBTITLE 1,Read CSV from Volume


def get_row_count(dframe):
    """Return an integer row count for a DataFrame. Ensures result is always a Python int."""
    try:
        return int(dframe.count())
    except Exception as e1:
        try:
            agg_result = dframe.selectExpr("count(1) as cnt").collect()
            if agg_result and len(agg_result) > 0:
                return int(agg_result[0][0])
        except Exception as e2:
            print(f"Warning: Could not compute row count. Errors: {e1}, {e2}")
        return 0


def read_csv_from_volume(csv_path, schema):
    """Read CSV file from Unity Catalog volume"""
    try:
        df = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).csv(csv_path)

        row_count = get_row_count(df)
        print(f"Successfully read {row_count} rows from {csv_path}")
        return df
    except Exception as e:
        print(f"Error reading {csv_path}: {str(e)}")
        raise


# Read customer data
customers_df = read_csv_from_volume(f"/Volumes/{source_volume}/customers_dim.csv", customer_schema)

# Read product data
products_df = read_csv_from_volume(f"/Volumes/{source_volume}/products_dim.csv", product_schema)

# Read transaction data
transactions_df = read_csv_from_volume(f"/Volumes/{source_volume}/transactions_fact.csv", transaction_schema)

# COMMAND ----------


# DBTITLE 1,Add Metadata Columns
def add_bronze_metadata(df, table_name):
    """Add metadata columns for Bronze layer with null-safe checksum"""
    null_safe_cols = [coalesce(col(c).cast("string"), lit("__NULL__")) for c in df.columns]
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("etl_batch_id", lit(run_date))
        .withColumn("source_system", lit("csv_volume"))
        .withColumn("source_file", lit(f"{table_name}.csv"))
        .withColumn("record_checksum", sha2(concat_ws("||", *null_safe_cols), 256))
    )


# Add metadata to each dataframe
customers_bronze = add_bronze_metadata(customers_df, "customers")
products_bronze = add_bronze_metadata(products_df, "products")
transactions_bronze = add_bronze_metadata(transactions_df, "transactions")

# COMMAND ----------


# DBTITLE 1,Write to Bronze Delta Tables
def write_bronze_table(df, table_name, catalog, schema):
    """Write DataFrame to Bronze Delta table"""
    table_path = f"`{catalog}`.`{schema}`.`{table_name}`"

    # Create catalog and schema if they don't exist
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as e:
        print(f"Catalog check: {e}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    # Compute row count before write (supports Spark Connect)
    row_count = get_row_count(df)

    # Write with merge schema for evolution
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").option(
        "overwriteSchema", "true"
    ).saveAsTable(table_path)

    print(f"Successfully wrote {row_count} rows to {table_path}")

    # Return table info for logging
    return {"table_name": table_name, "row_count": row_count, "target_path": table_path}


# Write bronze tables
bronze_tables = []

bronze_tables.append(write_bronze_table(customers_bronze, "bronze_customers", catalog, schema))

bronze_tables.append(write_bronze_table(products_bronze, "bronze_products", catalog, schema))

bronze_tables.append(write_bronze_table(transactions_bronze, "bronze_transactions", catalog, schema))

# COMMAND ----------

# DBTITLE 1,Log Results
# Create summary DataFrame for logging
summary_data = [(t["table_name"], t["row_count"], t["target_path"]) for t in bronze_tables]
summary_schema = StructType(
    [
        StructField("table_name", StringType()),
        StructField("row_count", LongType()),
        StructField("target_path", StringType()),
    ]
)

summary_df = spark.createDataFrame(summary_data, summary_schema)

# Display summary
display(summary_df)

# Log to job output
print("=" * 80)
print("BRONZE LAYER INGESTION COMPLETED")
print("=" * 80)
for table in bronze_tables:
    print(f"✓ {table['table_name']}: {table['row_count']:,} rows -> {table['target_path']}")

sum_of_rows = int(builtins.sum(t["row_count"] for t in bronze_tables))
print(f"\nTotal Tables Processed: {len(bronze_tables)}")
print(f"Total Rows Processed: {sum_of_rows:,}")

# COMMAND ----------

# DBTITLE 1,Return Success
# Return success for Lakeflow job dependency
dbutils.notebook.exit(
    json.dumps(
        {
            "status": "success",
            "tables_processed": int(len(bronze_tables)),
            "total_rows": sum_of_rows,
            "run_date": run_date,
        }
    )
)
