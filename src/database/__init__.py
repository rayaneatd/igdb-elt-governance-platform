"""
Database module for the Azure Market Insights ELT pipeline.

This module provides functions for interacting with the database, including:
- Initializing the database engine
- Executing SQL queries
- Reading from the database
- Updating the database
- Logging ingestion runs
- Logging batch executions
- Logging schema changes
- Getting checkpoints
- Upserting checkpoints
- Getting recent schema hashes
- Getting recent columns snapshots
- Getting pending fallback events
- Updating fallback event status
- Upserting fallback checkpoints
"""


from .types import TableCheckpoint, FallbackEvent, AnalyticsTask, RawBatchRef

from .auth import init_database_engine

from .core import (
    execute_sql_from_string,
    execute_sql_from_file,
    read_from_db,
    update_into_db
)

from .logs import (
    start_ingestion_run,
    complete_ingestion_run,
    cleanup_orphan_runs,
    log_batch,
    log_schema_change,
    get_checkpoints,
    upsert_checkpoint,
    get_recent_schema_hash,
    get_recent_columns_snapshot,
    get_unconsumed_raw_batches,
    get_unconsumed_raw_batch_refs
)

from .fallback import (
    get_pending_fallback_events,
    update_fallback_event_status,
    upsert_fallback_checkpoint
)

from .analytics import ingest_batches_to_postgres, consume_batches_to_postgres

