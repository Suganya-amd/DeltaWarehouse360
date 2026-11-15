"""SCD Type 2 Processor for Delta Lake and PySpark DataFrames."""

from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_date,
    current_timestamp,
    date_sub,
    lit,
    max as spark_max,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.window import Window


class SCD2Processor:
    """Helper class for implementing Slowly Changing Dimension Type 2 (SCD2) logic in Delta Lake."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def apply_scd2_logic(
        self,
        incoming_df: DataFrame,
        existing_df: Optional[DataFrame],
        business_key_cols: List[str],
        scd_columns: List[str],
        effective_date_col: str = "start_date",
    ) -> DataFrame:
        """
        Applies SCD Type 2 logic between incoming records and existing dimension records.

        Args:
            incoming_df: New/incoming data to process.
            existing_df: Existing dimension table DataFrame (None if initial load).
            business_key_cols: List of business key column names.
            scd_columns: List of columns to track for changes.
            effective_date_col: Column containing the effective start date.

        Returns:
            DataFrame containing all dimension records (historical expired + current).
        """
        incoming_cols = set(incoming_df.columns)
        incoming = incoming_df

        if "is_current" not in incoming_cols:
            incoming = incoming.withColumn("is_current", lit(1))
        if "is_current_record" not in incoming_cols:
            incoming = incoming.withColumn("is_current_record", col("is_current"))
        if "end_date" not in incoming_cols:
            incoming = incoming.withColumn("end_date", lit(None).cast("date"))
        if effective_date_col not in incoming_cols:
            incoming = incoming.withColumn(effective_date_col, current_date())

        # Safely verify existing DataFrame
        if existing_df is not None:
            try:
                base_columns = existing_df.columns
                existing_cols = set(base_columns)
            except Exception:
                existing_df = None

        # If initial load (no existing dimension data)
        if existing_df is None:
            return incoming.withColumn(
                "is_current_record",
                coalesce(col("is_current_record"), col("is_current"), lit(1)),
            ).withColumn(
                "is_current",
                coalesce(col("is_current"), lit(1)),
            )

        # Existing dimension data present: Separate historical vs current
        current_col = "is_current" if "is_current" in existing_cols else "is_current_record"

        existing_historical = existing_df.filter(col(current_col) == 0).select(*base_columns)
        existing_current = existing_df.filter(col(current_col) == 1)

        # Consider only current records from incoming for SCD2 comparison
        inc_current_filter = col("is_current") == 1 if "is_current" in incoming_cols else lit(True)
        inc_current = incoming.filter(inc_current_filter)

        # Change condition: any tracked column has changed (null-safe comparison)
        change_conditions = [
            (~col(f"new.{c}").eqNullSafe(col(f"old.{c}")))
            for c in scd_columns
            if c in incoming_cols and c in existing_cols
        ]
        if not change_conditions:
            has_changed_expr = lit(False)
        else:
            has_changed_expr = change_conditions[0]
            for cond in change_conditions[1:]:
                has_changed_expr = has_changed_expr | cond

        # Join incoming current with existing current
        join_expr = [col(f"new.{k}") == col(f"old.{k}") for k in business_key_cols]
        combined_join = join_expr[0]
        for je in join_expr[1:]:
            combined_join = combined_join & je

        joined_df = inc_current.alias("new").join(
            existing_current.alias("old"),
            combined_join,
            "full_outer",
        )

        # 1. Unchanged existing records
        unchanged_cols = [col(f"old.{c}").alias(c) for c in base_columns]
        unchanged_records = joined_df.filter(
            col(f"old.{business_key_cols[0]}").isNotNull()
            & (col(f"new.{business_key_cols[0]}").isNull() | (~has_changed_expr))
        ).select(*unchanged_cols)

        # 2. Expired records
        expired_date_expr = date_sub(coalesce(col(f"new.{effective_date_col}"), current_date()), 1)
        expired_cols = []
        for c in base_columns:
            if c == "end_date":
                expired_cols.append(expired_date_expr.alias("end_date"))
            elif c in ("is_current", "is_current_record"):
                expired_cols.append(lit(0).alias(c))
            elif c == "last_updated":
                expired_cols.append(current_timestamp().alias("last_updated"))
            else:
                expired_cols.append(col(f"old.{c}").alias(c))

        expired_records = joined_df.filter(
            col(f"old.{business_key_cols[0]}").isNotNull()
            & col(f"new.{business_key_cols[0]}").isNotNull()
            & has_changed_expr
        ).select(*expired_cols)

        # 3. New records & new versions
        new_cols = []
        for c in base_columns:
            if c == "end_date":
                new_cols.append(lit(None).cast("date").alias("end_date"))
            elif c in ("is_current", "is_current_record"):
                new_cols.append(lit(1).alias(c))
            elif c == "last_updated":
                new_cols.append(current_timestamp().alias("last_updated"))
            elif c in incoming_cols:
                new_cols.append(col(f"new.{c}").alias(c))
            else:
                new_cols.append(lit(None).alias(c))

        new_records = joined_df.filter(
            col(f"new.{business_key_cols[0]}").isNotNull()
            & (col(f"old.{business_key_cols[0]}").isNull() | has_changed_expr)
        ).select(*new_cols)

        # Union: prior historical + unchanged + expired + new
        result_df = (
            existing_historical.unionByName(unchanged_records).unionByName(expired_records).unionByName(new_records)
        )

        return result_df

    def generate_surrogate_keys(
        self,
        df: DataFrame,
        table_name: str,
        sk_col_name: str,
        order_cols: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Generates unique, deterministic surrogate keys for the dimension DataFrame.

        Args:
            df: Dimension DataFrame.
            table_name: Target dimension table name (for logging/identification).
            sk_col_name: Name of the surrogate key column.
            order_cols: Optional column names to sort by for deterministic keys.

        Returns:
            DataFrame with surrogate key column populated.
        """
        if order_cols and all(c in df.columns for c in order_cols):
            sort_spec = [col(c) for c in order_cols]
        elif "start_date" in df.columns:
            sort_spec = [col("start_date"), monotonically_increasing_id()]
        else:
            sort_spec = [monotonically_increasing_id()]

        # If surrogate key already exists, only assign keys to new rows with null SK
        if sk_col_name in df.columns:
            null_count = df.filter(col(sk_col_name).isNull()).limit(1).count()
            if null_count == 0:
                return df

            existing_rows = df.filter(col(sk_col_name).isNotNull())
            existing_max_row = existing_rows.agg(spark_max(col(sk_col_name)).alias("max_sk")).collect()
            base_sk = (
                int(existing_max_row[0]["max_sk"])
                if existing_max_row and existing_max_row[0]["max_sk"] is not None
                else 1000
            )

            w = Window.orderBy(*sort_spec)
            assigned_df = df.filter(col(sk_col_name).isNull()).withColumn(
                sk_col_name,
                (row_number().over(w) + base_sk).cast("long"),
            )
            return existing_rows.unionByName(assigned_df)

        w = Window.orderBy(*sort_spec)
        return df.withColumn(sk_col_name, (row_number().over(w) + 1000).cast("long"))
