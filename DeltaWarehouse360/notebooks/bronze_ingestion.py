# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC
# MAGIC **Task**: Read CSV files from volume and load to Bronze Delta tables

# COMMAND ----------

# DBTITLE 1,Import Libraries
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
def read_csv_from_volume(csv_path, schema):
    """Read CSV file from Unity Catalog volume"""
    try:
        df = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).csv(csv_path)

        print(f"Successfully read {df.count()} rows from {csv_path}")
        return df
    except Exception as e:
        print(f"Error reading {csv_path}: {str(e)}")
        raise


# Read customer data
customers_df = read_csv_from_volume(f"/Volumes/{source_volume}/customers_scd2.csv", customer_schema)

# Read product data
products_df = read_csv_from_volume(f"/Volumes/{source_volume}/products_scd2.csv", product_schema)

# Read transaction data
transactions_df = read_csv_from_volume(f"/Volumes/{source_volume}/transactions.csv", transaction_schema)

# COMMAND ----------


# DBTITLE 1,Add Metadata Columns
def add_bronze_metadata(df, table_name):
    """Add metadata columns for Bronze layer"""
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("etl_batch_id", lit(run_date))
        .withColumn("source_system", lit("csv_volume"))
        .withColumn("source_file", lit(f"{table_name}.csv"))
        .withColumn("record_checksum", sha2(concat_ws("||", *df.columns), 256))
    )


# Add metadata to each dataframe
customers_bronze = add_bronze_metadata(customers_df, "customers")
products_bronze = add_bronze_metadata(products_df, "products")
transactions_bronze = add_bronze_metadata(transactions_df, "transactions")

# COMMAND ----------


# DBTITLE 1,Write to Bronze Delta Tables
def write_bronze_table(df, table_name, catalog, schema):
    """Write DataFrame to Bronze Delta table"""
    table_path = f"{catalog}.{schema}.{table_name}"

    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Write with merge schema for evolution
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").option(
        "overwriteSchema", "true"
    ).saveAsTable(table_path)

    print(f"Successfully wrote {df.count()} rows to {table_path}")

    # Return table info for logging
    return {"table_name": table_name, "row_count": df.count(), "target_path": table_path}


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

# COMMAND ----------

# DBTITLE 1,Return Success
# Return success for Lakeflow job dependency
dbutils.notebook.exit(
    {
        "status": "success",
        "tables_processed": len(bronze_tables),
        "total_rows": sum(t["row_count"] for t in bronze_tables),
        "run_date": run_date,
    }
)
