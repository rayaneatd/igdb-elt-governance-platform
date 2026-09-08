import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.database.types import RawBatchRef, TableCheckpoint, AnalyticsTask
from src.database.logs import get_unconsumed_raw_batch_refs


class TestBatchTracking(unittest.TestCase):

    def test_types_support_last_batch_id(self):
        ckpt = TableCheckpoint(
            table_name="games",
            current_watermark=1700000000,
            fallback_watermark=1600000000,
            last_id=1234,
            offset_val=0,
            is_override_active=False,
            last_batch_id=42
        )
        self.assertEqual(ckpt.last_batch_id, 42)

        task = AnalyticsTask(
            start_watermark=1700000000,
            end_watermark=None,
            is_fallback=False,
            event_id=None,
            last_batch_id=100
        )
        self.assertEqual(task.last_batch_id, 100)

    def test_raw_batch_ref_construction(self):
        now = datetime(2026, 9, 8, 20, 0, 0, tzinfo=timezone.utc)
        ref = RawBatchRef(
            batch_id=55,
            path="IGDB/games/year=2026/month=09/day=08/1700000000_500.json",
            cursor_value=1700000000,
            offset_value=500,
            created_at=now
        )
        self.assertEqual(ref.batch_id, 55)
        self.assertEqual(ref.offset_value, 500)
        self.assertIn("1700000000_500.json", ref.path)

    def test_get_unconsumed_raw_batch_refs_query_structure(self):
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cur = MagicMock()

        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        row_item = MagicMock()
        row_item.batch_id = 12
        row_item.cursor_value = 1700000000
        row_item.offset_value = 0
        row_item.created_at = datetime(2026, 9, 8, tzinfo=timezone.utc)

        mock_cur.fetchall.return_value = [row_item]

        refs = get_unconsumed_raw_batch_refs(
            pool=mock_pool,
            table_name="GameSchema",
            endpoint="/games",
            last_batch_id=10,
            limit=5
        )

        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].batch_id, 12)

        # Ensure batch_id > last_batch_id is passed in the query
        executed_query, executed_params = mock_cur.execute.call_args[0]
        self.assertIn("batch_id > %(last_batch_id)s", executed_query)
        self.assertEqual(executed_params["last_batch_id"], 10)


if __name__ == "__main__":
    unittest.main()
