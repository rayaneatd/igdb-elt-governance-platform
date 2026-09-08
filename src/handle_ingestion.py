# 1. stdlib
from dataclasses import dataclass
import json
import time
import traceback
import uuid
from typing import Any, Literal
from datetime import datetime, timezone

# 2. third party
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from psycopg_pool import ConnectionPool

# 3. local 
from src.database import (
    complete_ingestion_run,
    get_checkpoints,
    get_pending_fallback_events,
    get_recent_columns_snapshot,
    get_recent_schema_hash,
    log_batch,
    log_schema_change,
    start_ingestion_run,
    update_fallback_event_status,
    upsert_checkpoint,
    upsert_fallback_checkpoint,
)
from src.datalake.functions import Containers, write_into_raw
from src.igdb.client import extract_igdb_data
from src.igdb.rate_limit import TokenBucket
from src.igdb.models import BaseIGDBSchema, BASE_IGDB_URL
from src.utils.alerting import AlertLevel, log_to_discord
from src.utils.types import ChangedColumns, TypeChange

# ================================================================
# 0. CONFIG & CONSTANTS
# ================================================================

bucket = TokenBucket(capacity=4, fill_rate=4) # 4 req/s


@dataclass(slots=True)
class IngestionTask:
    """Represents resolved ingestion parameter state for a table."""
    cursor: int
    end_watermark: int | None
    last_id: int
    offset: int
    is_fallback_event: bool
    event_id: str | None


# ================================================================
# 1. CHECKPOINT / FALLBACK
# ================================================================

def _construct_tables_dict(db_pool: ConnectionPool) -> dict[type, IngestionTask]:
    """
    Checks for PENDING fallback events and standard checkpoints per table.
    Pure read-only query without side effects.
    """
    defined_classes = BaseIGDBSchema.__subclasses__()
    pending_events = get_pending_fallback_events(db_pool, layer="RAW")
    checkpoints = get_checkpoints(db_pool, layer="RAW")

    fallback_map = {event.table_name: event for event in pending_events}

    resolved: dict[type, IngestionTask] = {}

    for cls in defined_classes:
        name = cls.__name__

        # 1. Table has a PENDING fallback event
        if name in fallback_map:
            event = fallback_map[name]
            resolved[cls] = IngestionTask(
                cursor=event.start_watermark,
                end_watermark=event.end_watermark,
                last_id=0,
                offset=0,
                is_fallback_event=True,
                event_id=event.event_id
            )
        # 2. Standard continuous incremental checkpoint
        else:
            ckpt = checkpoints.get(name)
            current_ts = ckpt.current_watermark if (ckpt and ckpt.current_watermark > 0) else cls._starting_point
            resolved[cls] = IngestionTask(
                cursor=current_ts,
                end_watermark=None,
                last_id=ckpt.last_id if ckpt else 0,
                offset=ckpt.offset_val if ckpt else 0,
                is_fallback_event=False,
                event_id=None
            )

    return resolved


# ================================================================
# 2. SCHEMA DRIFT 
# ================================================================


def get_schema_diff(
    logged_columns: dict[str, str],
    current_columns: dict[str, str]
) -> ChangedColumns:
        # first run
    if not logged_columns:
        return ChangedColumns(
            added=list(current_columns.keys()),
            removed=[],
            type_changed={}
        )

        # else
    return ChangedColumns(
            #? the fields we added
        added=list(current_columns.keys() - logged_columns.keys()),
            #? the fields we removed
        removed=list(logged_columns.keys() - current_columns.keys()),
            #? if data types are changed
        type_changed={
            col: TypeChange(old=logged_columns[col], new=current_columns[col])
            for col in logged_columns.keys() & current_columns.keys()
            if logged_columns[col] != current_columns[col]
    }
    )             


def detect_and_log_schema_change(
    db_pool: ConnectionPool,
    Model: Any,
    run_id: str,
    recent_schema_hash: str | None,
    current_schema_hash: str
):
    """Compare the current table schema against the last logged one and
    record any drift.
 
    No-op if the schema hash hasn't changed. Never raises: failures here are
    reported to Discord and swallowed, since schema logging must not block
    ingestion.
 
    Args:
        db_pool: Postgres connection pool.
        Model: Schema class of the table being checked.
        run_id: ID of the current ingestion run (for audit logging).
        recent_schema_hash: Hash last recorded for this table, or None.
        current_schema_hash: Hash of the table's current schema definition.
    """
    # schema change detection
    try:
        
        if recent_schema_hash != current_schema_hash:
            # we're going to compare the present with the past schema
            logged_columns: dict[str, str] = get_recent_columns_snapshot(db_pool, Model.__name__)
            current_columns: dict[str, str] = Model.get_columns_snapshot(format='dict')
            current_changed = get_schema_diff(logged_columns, current_columns)
            
            log_schema_change(
                db_pool,
                Model.__name__,
                current_schema_hash,
                current_columns,
                current_changed,
                run_id
            )  

            # TODO: add a fallback event for the past until the change, once it's done also btw update the fallback event layer to analytics, recycling is peak
    except Exception as schema_err:
        log_to_discord(
            f"Schema change detection failed for table {Model.__name__}: {schema_err}",
            AlertLevel.ERROR
        )


# ================================================================
# 3. RAW PERSISTENCE + CORE INGESTION
# ================================================================


def _save_raw_batch(azure_client: DataLakeServiceClient | BlobServiceClient, 
                    Model: type, 
                    batch: list[dict], 
                    cursor: int, 
                    offset: int,
                    now: datetime
                    ) -> None:
    """
    Persists a raw page of records to the bronze/raw zone in ADLS.
    Path pattern keeps pages uniquely addressable and roughly time-ordered.
    """

    path = f"IGDB/{str(Model._endpoint).lstrip('/')}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{cursor}_{offset}.json"

    write_into_raw(
        azure_client,
        Containers.Data.value,
        path,
        json.dumps(batch).encode()
    )



def _ingest_tables(
    db_pool: ConnectionPool,
    azure_client,
    Model: Any,
    task: IngestionTask,
    run_id: str
):
    """
    Ingest a single IGDB table with resume + fallback support.

    Args:
        task: IngestionTask dataclass with cursor, offset, last_id, end_watermark, is_fallback_event, event_id
    Returns:
        True if success, False if failed (logs to discord)

    Logic:
        - paginates by 500, resets offset to 0 + cursor=max_seen at 10000
        - updates checkpoints only if not fallback
        - closes fallback event when max_seen >= end_watermark or batch < 500
    """

    cursor = task.cursor
    end_watermark = task.end_watermark
    last_id = task.last_id
    offset = task.offset
    is_fallback_event = task.is_fallback_event
    event_id = task.event_id
    max_seen = cursor
    event_records_count = 0

    if is_fallback_event and event_id:
        update_fallback_event_status(db_pool, event_id, status="IN_PROGRESS")

    detect_and_log_schema_change(
        db_pool=db_pool,
        Model=Model,
        run_id=run_id,
        recent_schema_hash=get_recent_schema_hash(db_pool, Model.__name__),
        current_schema_hash=Model.get_signature()
    )

    # batch processing
    try:
        while True:
            bucket.acquire()
            query = Model.build_query(
                last_update_value=cursor,
                offset=offset
            )
            
            start_time = time.perf_counter()
            batch = []
            batch_status = "SUCCESS"
            batch_err = None
            now = datetime.now(timezone.utc)
            
            try:
                batch = extract_igdb_data(
                    url=f"{BASE_IGDB_URL}{Model._endpoint}",
                    query=query,
                    timeout=10
                )
            except Exception as extract_err:
                batch_status = "FAILED"
                batch_err = str(extract_err)
                if is_fallback_event and event_id:
                    update_fallback_event_status(
                        db_pool,
                        event_id,
                        status="FAILED",
                        error_message=batch_err
                    )
                raise
            finally:
                elapsed_ms = int((time.perf_counter() - start_time) * 1000)
                log_batch(
                    pool=db_pool,
                    run_id=run_id,
                    table_name=Model.__name__,
                    layer="RAW",
                    status=batch_status,
                    cursor_value=cursor,
                    offset_value=offset,
                    records_count=len(batch) if batch else 0,
                    duration_ms=elapsed_ms,
                    query_sent=query,
                    created_at=now,
                    error_message=batch_err
                )
            if not batch:
                if is_fallback_event and event_id:
                    update_fallback_event_status(
                        db_pool,
                        event_id,
                        status="COMPLETED",
                        records_processed=event_records_count
                    )
                else:
                    # Standard incremental completion update
                    upsert_checkpoint(
                        pool=db_pool,
                        table_name=Model.__name__,
                        current_watermark=max_seen,
                        last_id=last_id,
                        layer="RAW",
                        offset_val=0,
                        run_id=run_id
                    )
                    upsert_fallback_checkpoint(
                        pool=db_pool, 
                        table_name=Model.__name__,
                        layer="RAW",
                        fallback_watermark=max_seen)
                break
            # Validate records
            for record in batch:
                if "updated_at" not in record or "id" not in record:
                    raise ValueError(
                        f"Record missing 'updated_at' or 'id' for {Model.__name__}: {record}"
                    )
            # Persist raw page to ADLS Raw zone
            _save_raw_batch(
                azure_client=azure_client, 
                Model=Model, 
                batch=batch, 
                cursor=cursor, 
                offset=offset,
                now=now)
            event_records_count += len(batch)
            batch_max_ts = max(r["updated_at"] for r in batch)
            batch_max_id_for_ts = max(r["id"] for r in batch if r["updated_at"] == batch_max_ts)
            if batch_max_ts > max_seen:
                max_seen = batch_max_ts
                last_id = batch_max_id_for_ts
            else:
                last_id = max(last_id, batch_max_id_for_ts)
            # Check if reached specified end_watermark for fallback event
            if is_fallback_event and end_watermark and max_seen >= end_watermark:
                update_fallback_event_status(
                    db_pool,
                    event_id,
                    status="COMPLETED",
                    records_processed=event_records_count
                )
                break
            if len(batch) < 500:
                if is_fallback_event and event_id:
                    update_fallback_event_status(
                        db_pool,
                        event_id,
                        status="COMPLETED",
                        records_processed=event_records_count
                    )
                else:
                    upsert_checkpoint(
                        pool=db_pool,
                        table_name=Model.__name__,
                        current_watermark=max_seen,
                        last_id=last_id,
                        layer="RAW",
                        offset_val=0,
                        run_id=run_id
                    )
                    upsert_fallback_checkpoint(
                        pool=db_pool, 
                        table_name=Model.__name__, 
                        layer="RAW",
                        fallback_watermark=max_seen
                    )
                break
            # Compute next resume state for progressive pagination
            next_offset = offset + 500
            next_cursor = max_seen if next_offset >= 10000 else cursor
            if next_offset >= 10000:
                next_offset = 0
            if not is_fallback_event:
                upsert_checkpoint(
                    pool=db_pool,
                    table_name=Model.__name__,
                    current_watermark=next_cursor,
                    last_id=last_id,
                    layer="RAW",
                    offset_val=next_offset,
                    run_id=run_id
                )
            offset = next_offset
            cursor = next_cursor
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Unexpected failure ingesting {Model.__name__}: {e}\n{tb}"
        max_len = 1900
        if len(msg) > max_len:
            msg = msg[:200] + "\n...\n" + msg[-(max_len - 200):]
        log_to_discord(msg=f"```\n{msg}\n```", level=AlertLevel.WARNING)
        

# ================================================================
# 4. MAIN ORCHESTRATOR
# ================================================================

def do_ingestion(azure_client: DataLakeServiceClient | BlobServiceClient, db_pool: ConnectionPool) -> None:
    """
    Main ingestion pipeline. Processes pending fallback events or continuous incremental watermarks,
    fetches data from IGDB API, persists raw batches to ADLS, and updates audit state in Postgres.
    """
    run_id = str(uuid.uuid4())
    start_ingestion_run(db_pool, run_id, layer="RAW")

    run_status: Literal["COMPLETED", "FAILED"] = "COMPLETED"
    run_error = None

    try:
        tables = _construct_tables_dict(db_pool)

        for Model, task in tables.items():
            _ingest_tables(
                db_pool=db_pool,
                azure_client=azure_client,
                Model=Model,
                task=task,
                run_id=run_id
            )

    # exception
    except Exception as e:
        run_status = "FAILED"
        run_error = str(e)
        log_to_discord(f"Pipeline orchestration run failed: {e}", level=AlertLevel.ERROR)
    finally:
        complete_ingestion_run(db_pool, run_id, run_status, run_error)