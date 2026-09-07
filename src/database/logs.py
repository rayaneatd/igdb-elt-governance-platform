import msgspec

from datetime import datetime
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row, namedtuple_row
from typing import Literal, Any

from .core import _execute
from .types import TableCheckpoint
from src.utils.types import ChangedColumns, TypeChange
from src.utils.alerting import (
    log_to_discord, AlertLevel
)

# ================================================================
# INGESTION
# ================================================================

def start_ingestion_run(
    pool: ConnectionPool, 
    run_id: str, 
    error_message: str | None = None,
    layer: str | None = None
) -> None:
    """
    Logs the start of an orchestration run.
    
    Args:
        pool: The database connection pool.
        run_id: The run ID.
        error_message: Optional initial error message.
        layer: The pipeline layer name.
    """
    query = """
        INSERT INTO logs.ingestion_runs (run_id, started_at, status, error_message, layer)
        VALUES (%(run_id)s, CURRENT_TIMESTAMP, 'RUNNING', %(error_message)s, %(layer)s)
        ON CONFLICT (run_id) DO NOTHING;
    """
    _execute(pool, query, {"run_id": run_id, "error_message": error_message, "layer": layer})

def complete_ingestion_run(
    pool: ConnectionPool, 
    run_id: str, 
    status: Literal["COMPLETED", "FAILED"], 
    error_message: str | None = None
) -> None:
    """
    Logs the completion or failure of an orchestration run.
    
    Args:
        pool: The database connection pool.
        run_id: The run ID.
        status: The status of the run ('COMPLETED' or 'FAILED').
        error_message: The error message if failed.
    """
    query = """
        UPDATE logs.ingestion_runs
        SET completed_at = CURRENT_TIMESTAMP,
            status = %(status)s,
            error_message = %(error_message)s
        WHERE run_id = %(run_id)s;
    """
    _execute(pool, query, {
        "run_id": run_id,
        "status": status,
        "error_message": error_message
    })

def log_batch(
    pool: ConnectionPool,
    run_id: str,
    table_name: str,
    layer: str,
    status: str,
    cursor_value: int,
    offset_value: int,
    records_count: int,
    duration_ms: int,
    query_sent: str,
    created_at: datetime,
    error_message: str | None = None
) -> None:
    """Logs an individual batch execution details."""
    query = """
        INSERT INTO logs.batch_logs (
            run_id, table_name, layer, status, cursor_value, offset_value,
            records_count, duration_ms, query_sent, error_message, created_at
        )
        VALUES (
            %(run_id)s, %(table_name)s, %(layer)s, %(status)s, %(cursor_value)s,
            %(offset_value)s, %(records_count)s, %(duration_ms)s, %(query_sent)s,
            %(error_message)s, %(created_at)s
        );
    """
    _execute(pool, query, {
        "run_id": run_id,
        "table_name": table_name,
        "layer": layer,
        "status": status,
        "cursor_value": cursor_value,
        "offset_value": offset_value,
        "records_count": records_count,
        "duration_ms": duration_ms,
        "query_sent": query_sent,
        "error_message": error_message,
        "created_at": created_at
    })


# ================================================================
# SCHEMA
# ================================================================

def log_schema_change(
    pool: ConnectionPool,
    table_name: str,
    schema_hash: str,
    columns_snapshot: dict[str, str],
    changed_columns: ChangedColumns,
    run_id: str,
    status: str = "NEW_COLUMN",
    action_taken: str | None = None
) -> None:
    """Logs schema changes or drifts found during parsing."""
    query = """
        INSERT INTO logs.schema_history (table_name, schema_hash, columns_snapshot, changed_columns, detected_in_run_id, status, action_taken)
        VALUES (%(table_name)s, %(schema_hash)s, %(columns_snapshot)s, %(changed_columns)s, %(run_id)s, %(status)s, %(action_taken)s)
        ON CONFLICT DO NOTHING;
    """
    _execute(pool, query, {
        "table_name": table_name,
        "schema_hash": schema_hash,
        "columns_snapshot": msgspec.json.encode(columns_snapshot).decode("utf-8"),
        "changed_columns": msgspec.json.encode(changed_columns).decode("utf-8"),
        "run_id": run_id,
        "status": status,
        "action_taken": action_taken
    })

def get_recent_schema_hash(pool: ConnectionPool, 
                           table_name: str
) -> str | None:
    """Retrieves the most recent schema hash for a table."""
    query = """
        SELECT schema_hash
        FROM logs.schema_history
        WHERE table_name = %(table_name)s
        ORDER BY detected_at DESC
        LIMIT 1;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(query, {"table_name": table_name})
            row = cur.fetchone()
            if row:
                return row.schema_hash # pyrefly: ignore
            return None

def get_recent_columns_snapshot(
    pool: ConnectionPool,
    table_name: str
) -> dict[str, str]:
    """
    Retrieves the most recent columns snapshot for a table.
    
    Args:
        pool: The database connection pool.
        table_name: The name of the table.
    """
    
    query = """
        SELECT columns_snapshot
        FROM logs.schema_history
        WHERE table_name = %(table_name)s
        ORDER BY detected_at DESC
        LIMIT 1;
    """

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(query, {"table_name": table_name})
            row = cur.fetchone()
            if not row or not row.columns_snapshot: # pyrefly: ignore
                return {}

            raw = row.columns_snapshot
            if isinstance(raw, dict):
                return raw
            
            try:
                return msgspec.json.decode(raw, type=dict[str, str]) # pyrefly: ignore
            except Exception:
                log_to_discord(
                    f"Failed to decode columns snapshot for table {table_name}",
                    AlertLevel.WARNING
                )
                return {}
    

# ================================================================
# CHECKPOINTS
# ================================================================

def get_checkpoints(
    pool: ConnectionPool, 
    layer: Literal["RAW", "ANALYTICS"] = "RAW"
) -> dict[str, TableCheckpoint]:
    """
    Retrieves all table checkpoints from Postgres.
    
    Args:
        pool: The database connection pool.
        layer: The pipeline layer ('RAW' or 'ANALYTICS').
    """
    query = """
        SELECT table_name, current_watermark, fallback_watermark, last_id, offset_val, is_override_active
        FROM logs.ingestion_checkpoints
        WHERE layer = %(layer)s;
    """
    
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, {"layer": layer})
            return {row["table_name"]: TableCheckpoint(**row) for row in cur.fetchall()}

def upsert_checkpoint(
    pool: ConnectionPool,
    table_name: str,
    current_watermark: int,
    last_id: int,
    layer: Literal["RAW", "ANALYTICS"],
    offset_val: int,
    run_id: str | None,
    is_override_active: bool = False
) -> None:
    """
    Upserts checkpoint status for a table.
    
    Args:
        pool: The database connection pool.
        table_name: The table name.
        current_watermark: The current watermark timestamp.
        last_id: The last processed ID.
        layer: The pipeline layer.
        offset_val: The pagination offset value.
        run_id: The run ID.
        is_override_active: Whether override mode is active.
    """
    query = """
        INSERT INTO logs.ingestion_checkpoints (
            table_name, current_watermark, last_id, layer, offset_val, last_successful_run_id, is_override_active, updated_at
        )
        VALUES (
            %(table_name)s, %(current_watermark)s, %(last_id)s, %(layer)s, %(offset_val)s, %(run_id)s, %(is_override_active)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (table_name, layer) DO UPDATE SET
            current_watermark = EXCLUDED.current_watermark,
            last_id = EXCLUDED.last_id,
            layer = EXCLUDED.layer,
            offset_val = EXCLUDED.offset_val,
            last_successful_run_id = EXCLUDED.last_successful_run_id,
            is_override_active = EXCLUDED.is_override_active,
            updated_at = CURRENT_TIMESTAMP;
    """
    _execute(pool, query, {
        "table_name": table_name,
        "current_watermark": current_watermark,
        "last_id": last_id,
        "layer": layer,
        "offset_val": offset_val,
        "run_id": run_id,
        "is_override_active": is_override_active
    })

def get_unconsumed_raw_batches(
    pool: ConnectionPool,
    table_name: str,
    endpoint: str,
    start_watermark: int,
    end_watermark: int | None = None,
    full_load: bool = False,
) -> list[str]:
    if full_load:
        base_query = """
            SELECT cursor_value, offset_value, created_at
            FROM logs.batch_logs
            WHERE table_name = %(table_name)s
              AND layer = 'RAW'
              AND status = 'SUCCESS'
            ORDER BY created_at DESC
            LIMIT 1;
        """
        params: dict[str, Any] = {"table_name": table_name}
    else:
        base_query = """
            SELECT cursor_value, offset_value, created_at
            FROM logs.batch_logs
            WHERE table_name = %(table_name)s
              AND layer = 'RAW'
              AND status = 'SUCCESS'
              AND cursor_value > %(start_watermark)s
        """
        params = {"table_name": table_name, "start_watermark": start_watermark}
        if end_watermark is not None:
            base_query += " AND cursor_value <= %(end_watermark)s"
            params["end_watermark"] = end_watermark
        base_query += " ORDER BY cursor_value ASC;"

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(base_query, params)
            rows = cur.fetchall()

    endpoint_clean = endpoint.strip("/")
    return [ 
        f"IGDB/{endpoint_clean}/year={r.created_at.year}/" # pyrefly: ignore
        f"month={r.created_at.month:02d}/day={r.created_at.day:02d}/" 
        f"{r.cursor_value}_{r.offset_value}.json" 
        for r in rows
    ]