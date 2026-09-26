import json
from typing import Any
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from src.igdb.models import BaseIGDBSchema


def get_registered_tables() -> list[str]:
    """Returns the names of all registered IGDB model classes."""
    return sorted([cls.__name__ for cls in BaseIGDBSchema.__subclasses__()])


def get_dashboard_stats(pool: ConnectionPool) -> dict[str, Any]:
    """Retrieves high-level KPI metrics for the governance dashboard header."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM logs.ingestion_runs) AS total_runs,
                    (SELECT COUNT(*) FROM logs.ingestion_runs WHERE status = 'COMPLETED') AS successful_runs,
                    (SELECT COUNT(*) FROM logs.ingestion_runs WHERE status = 'FAILED') AS failed_runs,
                    (SELECT COUNT(*) FROM logs.ingestion_checkpoints) AS total_tables,
                    (SELECT COUNT(*) FROM logs.ingestion_checkpoints WHERE is_override_active = TRUE) AS active_overrides,
                    (SELECT COALESCE(SUM(records_count), 0) FROM logs.batch_logs) AS total_records_ingested,
                    (SELECT COUNT(*) FROM logs.schema_history) AS total_schema_changes,
                    (SELECT COUNT(*) FROM logs.fallback_events WHERE status = 'PENDING') AS pending_fallbacks
            """)
            row = cur.fetchone()
            return dict(row) if row else {}


def get_runs_history(pool: ConnectionPool, limit: int = 50) -> list[dict[str, Any]]:
    """Retrieves recent orchestration runs."""
    query = """
        SELECT run_id, started_at, completed_at, layer, status, error_message,
               EXTRACT(EPOCH FROM (completed_at - started_at))::INT AS duration_seconds
        FROM logs.ingestion_runs
        ORDER BY started_at DESC
        LIMIT %s;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (limit,))
            return [dict(row) for row in cur.fetchall()]


def get_all_checkpoints(pool: ConnectionPool) -> list[dict[str, Any]]:
    """Retrieves state of all checkpoints."""
    query = """
        SELECT table_name, current_watermark, fallback_watermark, last_id, layer,
               offset_val, is_override_active, last_successful_run_id, updated_at
        FROM logs.ingestion_checkpoints
        ORDER BY table_name ASC;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return [dict(row) for row in cur.fetchall()]


def set_checkpoint_override(
    pool: ConnectionPool,
    table_name: str,
    custom_watermark: int | None = None,
    activate_fallback: bool = True
) -> bool:
    """
    Triggers fallback override on a specific table checkpoint.
    """
    with pool.connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                if custom_watermark is not None:
                    cur.execute("""
                        UPDATE logs.ingestion_checkpoints
                        SET fallback_watermark = %s,
                            is_override_active = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = %s;
                    """, (custom_watermark, activate_fallback, table_name))
                else:
                    cur.execute("""
                        UPDATE logs.ingestion_checkpoints
                        SET is_override_active = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = %s;
                    """, (activate_fallback, table_name))
                return cur.rowcount > 0


def get_recent_batches(
    pool: ConnectionPool,
    limit: int = 100,
    table_name: str | None = None
) -> list[dict[str, Any]]:
    """Retrieves recent batch logs."""
    query = """
        SELECT batch_id, run_id, table_name, layer, status, cursor_value, offset_value,
               records_count, duration_ms, query_sent, error_message, created_at
        FROM logs.batch_logs
        WHERE (%s::VARCHAR IS NULL OR table_name = %s::VARCHAR)
        ORDER BY created_at DESC
        LIMIT %s;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (table_name, table_name, limit))
            return [dict(row) for row in cur.fetchall()]


def get_schema_drifts(pool: ConnectionPool) -> list[dict[str, Any]]:
    """
    Retrieves schema history entries and parses JSONB changed_columns
    to produce rich, readable drift diagnostics.
    """
    query = """
        SELECT id, table_name, schema_hash, columns_snapshot, changed_columns,
               detected_at, detected_in_run_id, included_at, status, action_taken
        FROM logs.schema_history
        ORDER BY detected_at DESC;
    """
    results = []
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

            for r in rows:
                item = dict(r)

                changes = item.get("changed_columns") or {}
                if isinstance(changes, str):
                    try:
                        changes = json.loads(changes)
                    except Exception:
                        changes = {}

                added = changes.get("added", [])
                removed = changes.get("removed", [])
                type_changed = changes.get("type_changed", {})

                summary_parts = []
                if added:
                    summary_parts.append(f"+{len(added)} col: {', '.join(added[:3])}{'...' if len(added) > 3 else ''}")
                if removed:
                    summary_parts.append(f"-{len(removed)} col: {', '.join(removed[:3])}{'...' if len(removed) > 3 else ''}")
                if type_changed:
                    summary_parts.append(f"~{len(type_changed)} type: {', '.join(type_changed.keys())}")

                item["added"] = added
                item["removed"] = removed
                item["type_changed"] = type_changed
                item["changes_summary"] = " | ".join(summary_parts) if summary_parts else "Initial Snapshot / Unchanged"

                item["column_name"] = ", ".join(added) if added else (", ".join(removed) if removed else "-")
                item["data_type"] = (
                    f"MODIFIED ({len(type_changed)})" if type_changed
                    else ("NEW COLUMN" if added else "UNCHANGED")
                )

                results.append(item)

    return results


def create_fallback_event(
    pool: ConnectionPool,
    table_name: str,
    start_watermark: int,
    end_watermark: int,
    layer: str = "RAW"
) -> dict[str, Any]:
    """Creates a new PENDING fallback event in logs.fallback_events."""
    query = """
        INSERT INTO logs.fallback_events (table_name, layer, start_watermark, end_watermark, status)
        VALUES (%s, %s, %s, %s, 'PENDING')
        RETURNING event_id::text, table_name, layer, start_watermark, end_watermark, status, created_at;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (table_name, layer, start_watermark, end_watermark))
            return dict(cur.fetchone())  # pyrefly: ignore


def get_fallback_events(pool: ConnectionPool, limit: int = 50) -> list[dict[str, Any]]:
    """Retrieves fallback events queue history."""
    query = """
        SELECT event_id::text, table_name, layer, start_watermark, end_watermark,
               status, records_processed, error_message, created_at, completed_at
        FROM logs.fallback_events
        ORDER BY created_at DESC
        LIMIT %s;
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (limit,))
            return [dict(row) for row in cur.fetchall()]
