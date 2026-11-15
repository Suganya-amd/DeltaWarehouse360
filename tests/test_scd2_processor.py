"""Unit tests for DeltaWarehouse360 SCD2Processor and main module."""

import sys
import unittest
from unittest.mock import MagicMock

# Mock PySpark if not natively available in this interpreter
try:
    import pyspark.sql.functions  # noqa: F401
except Exception:
    sys.modules.setdefault("pyspark", MagicMock())
    sys.modules.setdefault("pyspark.sql", MagicMock())
    sys.modules.setdefault("pyspark.sql.functions", MagicMock())
    sys.modules.setdefault("pyspark.sql.window", MagicMock())

from DeltaWarehouse360 import SCD2Processor, __version__
from DeltaWarehouse360.main import main


class TestDeltaWarehouse360(unittest.TestCase):
    def test_package_metadata(self):
        """Verify package version and exports."""
        self.assertEqual(__version__, "0.0.1")
        self.assertIsNotNone(SCD2Processor)

    def test_main(self):
        """Verify main entrypoint executes without error."""
        try:
            main()
        except Exception as e:
            self.fail(f"main() raised {e} unexpectedly!")

    def test_scd2_processor_initial_load(self):
        """Test initial load when existing dimension DataFrame is None."""
        mock_spark = MagicMock()
        processor = SCD2Processor(mock_spark)

        mock_incoming = MagicMock()
        mock_incoming.columns = ["customer_id", "customer_name", "customer_email", "start_date"]
        mock_incoming.withColumn.return_value = mock_incoming

        result = processor.apply_scd2_logic(
            incoming_df=mock_incoming,
            existing_df=None,
            business_key_cols=["customer_id"],
            scd_columns=["customer_email"],
            effective_date_col="start_date",
        )

        self.assertIsNotNone(result)
        self.assertGreaterEqual(mock_incoming.withColumn.call_count, 2)

    def test_scd2_processor_incremental_load(self):
        """Test incremental load joins incoming and existing datasets."""
        mock_spark = MagicMock()
        processor = SCD2Processor(mock_spark)

        mock_incoming = MagicMock()
        mock_incoming.columns = [
            "customer_id",
            "customer_name",
            "customer_email",
            "start_date",
            "is_current",
            "is_current_record",
            "end_date",
        ]

        mock_existing = MagicMock()
        mock_existing.columns = [
            "customer_id",
            "customer_name",
            "customer_email",
            "start_date",
            "is_current",
            "is_current_record",
            "end_date",
        ]

        mock_incoming_current = MagicMock()
        mock_incoming.filter.return_value = mock_incoming_current

        mock_joined = MagicMock()
        mock_incoming_current.alias.return_value.join.return_value = mock_joined

        mock_filtered = MagicMock()
        mock_joined.filter.return_value = mock_filtered
        mock_selected = MagicMock()
        mock_filtered.select.return_value = mock_selected
        mock_selected.withColumn.return_value = mock_selected

        mock_existing_hist = MagicMock()
        mock_existing_hist.select.return_value = mock_existing_hist
        mock_existing.filter.return_value = mock_existing_hist
        mock_existing_hist.unionByName.return_value = mock_existing_hist

        result = processor.apply_scd2_logic(
            incoming_df=mock_incoming,
            existing_df=mock_existing,
            business_key_cols=["customer_id"],
            scd_columns=["customer_email"],
            effective_date_col="start_date",
        )

        self.assertIsNotNone(result)
        self.assertTrue(mock_incoming_current.alias.called)

    def test_generate_surrogate_keys_new_table(self):
        """Test surrogate key generation when sk column is not present."""
        mock_spark = MagicMock()
        processor = SCD2Processor(mock_spark)

        mock_df = MagicMock()
        mock_df.columns = ["customer_id", "customer_name", "start_date"]
        mock_df.withColumn.return_value = mock_df

        result = processor.generate_surrogate_keys(
            df=mock_df,
            table_name="dim_customer",
            sk_col_name="customer_sk",
            order_cols=["customer_id", "start_date"],
        )

        self.assertIsNotNone(result)
        mock_df.withColumn.assert_called_once()


if __name__ == "__main__":
    unittest.main()
