# 1. native
import io
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
from typing import Literal

# 2. third party
import polars as pl
import patito as pt
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from psycopg_pool import ConnectionPool 

# 3. local
from src.igdb.models import BaseIGDBSchema
from src.database.core import (
    DatabaseSchema, 
    _validate_identifier,
    execute_sql_from_string, 
    update_into_db
)
from src.datalake.functions import (
    Containers,
    read_from_raw 
)
from .types import AnalyticsTask
from src.database.logs import (
    get_checkpoints, 
    upsert_checkpoint, 
    get_unconsumed_raw_batches,
    start_ingestion_run,
    complete_ingestion_run
)
from src.database.fallback import (
    get_pending_fallback_events, 
    update_fallback_event_status, 
    upsert_fallback_checkpoint
)
from src.utils.alerting import (
    AlertLevel, 
    log_to_discord
)
from src.utils.types import _LIST_RE


def _get_watermark_dict(db_pool: ConnectionPool) -> dict[type, AnalyticsTask]:
    """
    Retrieves execution parameters for the ANALYTICS layer per table.
    Combines fallback events for tables that have them with standard incremental checkpoints for the rest.
    Pure read-only query without side effects.
    """
    defined_classes = BaseIGDBSchema.__subclasses__()
    pending_events = get_pending_fallback_events(db_pool, layer='ANALYTICS')
    checkpoints = get_checkpoints(db_pool, layer='ANALYTICS')

    # Index pending fallback events by table_name for O(1) lookup
    fallback_map = {event.table_name: event for event in pending_events}

    resolved: dict[type, AnalyticsTask] = {}

    for cls in defined_classes:
        name = cls.__name__
        
        # 1. Table has a PENDING fallback event
        if name in fallback_map:
            event = fallback_map[name]
            resolved[cls] = AnalyticsTask(
                start_watermark=event.start_watermark,
                end_watermark=event.end_watermark,
                is_fallback=True,
                event_id=event.event_id
            )
        # 2. Standard continuous incremental checkpoint
        else:
            ckpt = checkpoints.get(name)
            current_ts = ckpt.current_watermark if (ckpt and ckpt.current_watermark > 0) else cls._starting_point
            resolved[cls] = AnalyticsTask(
                start_watermark=current_ts,
                end_watermark=None,
                is_fallback=False,
                event_id=None
            )

    return resolved


def ensure_schema(db_pool: ConnectionPool) -> None:
    """Ensures target Postgres tables and M2M join tables exist for all models."""
    classes = BaseIGDBSchema.__subclasses__()

    # Snapshot existing tables
    rows = execute_sql_from_string(
        pool=db_pool,
        query="SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
        params=(DatabaseSchema.ANALYTICS.value,),
        fetch=True
    )
    existing = {r[0] for r in rows} if rows else set()

    for cls in classes:
        base = cls._endpoint.strip('/').replace('/', '_')
        scd1 = base
        scd2 = f"{base}_scd2"
        target = cls.get_table_name()
        other = scd2 if target == scd1 else scd1

        if other in existing and target not in existing:
            raise RuntimeError(
                f"[SCD SWITCH BLOQUÉ] {base}: {other} existe déjà, tu veux créer {target}. "
                f"Fais une migration manuelle, pas un switch de flag."
            )

        query = cls.build_pg_query()
        execute_sql_from_string(pool=db_pool, query=query)
        sync_table_indexes(db_pool=db_pool, model=cls)


def sync_table_indexes(db_pool: ConnectionPool, model: type[BaseIGDBSchema]) -> None:
    """
    Synchronizes indexes for a given model schema in PostgreSQL.

    Dynamically creates newly added indexes and drops obsolete ones that match
    the model's managed naming prefix (idx_{table_name}_%).
    Primary keys, unique constraints, and foreign key junction tables are never dropped.
    """
    schema = DatabaseSchema.ANALYTICS.value
    table_name = model.get_table_name()
    expected_indexes = {
        model.get_index_name(cols): cols
        for cols in model.get_indexes()
    }

    # Query existing managed indexes on the target table
    query_existing = """
        SELECT i.relname AS index_name
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_constraint c ON c.conindid = ix.indexrelid
        WHERE n.nspname = %s
          AND t.relname = %s
          AND c.conindid IS NULL
          AND ix.indisprimary = FALSE
          AND i.relname LIKE %s;
    """
    managed_prefix = f"idx_{table_name}_%"
    rows = execute_sql_from_string(
        pool=db_pool,
        query=query_existing,
        params=(schema, table_name, managed_prefix),
        fetch=True
    )
    existing_indexes = {r[0] for r in rows} if rows else set()

    # Drop obsolete indexes removed from _index_at
    to_drop = existing_indexes - set(expected_indexes.keys())
    for idx_name in to_drop:
        _validate_identifier(idx_name, "index")
        execute_sql_from_string(
            pool=db_pool,
            query=f'DROP INDEX IF EXISTS "{schema}"."{idx_name}";'
        )

    # Create newly added indexes
    to_create = set(expected_indexes.keys()) - existing_indexes
    for idx_name in to_create:
        cols = expected_indexes[idx_name]
        cols_str = ", ".join(f'"{c}"' for c in cols)
        execute_sql_from_string(
            pool=db_pool,
            query=f'CREATE INDEX IF NOT EXISTS {idx_name} ON "{schema}"."{table_name}" ({cols_str});'
        )


def get_data_from_datalake(
    azure_client: DataLakeServiceClient | BlobServiceClient,
    db_pool: ConnectionPool,
    Model: type,
    task: AnalyticsTask,
) -> pl.DataFrame:
    """
    Reads raw bronze batches from ADLS for a specific model and watermark
    range, and returns them concatenated into a single DataFrame.
    """
    paths = get_unconsumed_raw_batches(
        pool=db_pool,
        table_name=Model.__name__,
        endpoint=Model._endpoint,
        start_watermark=task.start_watermark,
        end_watermark=task.end_watermark,
        full_load=Model._full_load
    )
 
    if not paths:
        return pl.DataFrame()
 
    def _download(path: str) -> bytes | None:
        return read_from_raw(azure_client, Containers.Data.value, path)
 
    with ThreadPoolExecutor(max_workers=8) as executor:
        raw_blobs = list(executor.map(_download, paths))
 
    missing = [p for p, b in zip(paths, raw_blobs) if b is None]
    if missing:
        log_to_discord(
            f"{len(missing)} batch(es) marked SUCCESS in batch_logs but "
            f"missing from ADLS for {Model.__name__}: {missing[:5]}"
            f"{'...' if len(missing) > 5 else ''}",
            AlertLevel.ERROR,
        )
        raise FileNotFoundError(
            f"Missing raw blob(s) for {Model.__name__}: {len(missing)} file(s)"
        )
 
    frames = [pl.read_json(io.BytesIO(b), infer_schema_length=None) for b in raw_blobs if b is not None]
    return pl.concat(frames, how='diagonal_relaxed') if frames else pl.DataFrame()


def _stringify_for_hash(df: pl.DataFrame, column: str) -> pl.Expr:
    dtype = df.schema[column]
    if isinstance(dtype, pl.List):
        return (
            pl.col(column)
            .list.eval(pl.element().cast(pl.Utf8))
            .list.sort()
            .list.join(",")
            .fill_null("")
        )
    return pl.col(column).cast(pl.Utf8).fill_null("")

def enforce_schema_and_types(Model: type, df: pl.DataFrame) -> pl.DataFrame:
    """
    Leverages schema predictability to align the DataFrame with Model fields:
    - Fills missing declared fields with nulls.
    - Retains only fields declared on Model.
    - Adds technical audit columns (_ingested_at, _hash).
    """
    if df.is_empty():
        return df

    expected_fields = list(Model.model_fields.keys())
    exprs = []

    for name in expected_fields:
        if name in df.columns:
            exprs.append(pl.col(name))
        else:
            exprs.append(pl.lit(None).alias(name))

    aligned_df = df.select(exprs)
    
    aligned_df = pt.DataFrame(aligned_df).set_model(Model).cast()

    # Compute row hashes for data lineage/change tracking
    hash_expr = pl.concat_str(
        [_stringify_for_hash(aligned_df, c) for c in expected_fields],
        separator="|"
    ).map_elements(lambda s: sha256(s.encode("utf-8")).hexdigest(), return_dtype=pl.Utf8)

    now_ts = datetime.now(timezone.utc)
    
    return aligned_df.with_columns([
        pl.lit(now_ts).alias("_ingested_at"),
        hash_expr.alias("_hash")
    ])


def deduplicate_records(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicates records on entity 'id', preserving the most recent record.
    Sorts by updated_at before deduplication to ensure deterministic output
    regardless of batch input order.
    """
    if df.is_empty() or "id" not in df.columns:
        return df
    if "updated_at" in df.columns:
        df = df.sort("updated_at", descending=False)
    return df.unique(subset=["id"], keep="last")


def extract_m2m_relationships(Model: type, df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    For non-SCD2 models (_conserve_history=False), extracts list fields (e.g. genres, platforms)
    into exploded DataFrames for junction tables (e.g. game_genres).
    """
    if df.is_empty() or Model._conserve_history or "id" not in df.columns:
        return {}

    base_name = Model._endpoint.strip('/').replace('/', '_')
    singular_base = Model._to_singular(base_name)
    m2m_dfs: dict[str, pl.DataFrame] = {}

    for field_name, info in Model.model_fields.items():
        pl_type = Model._convert_to_polar_types(info.annotation)
        match = _LIST_RE.match(pl_type)
        if not match:
            continue

        singular_rel = Model._to_singular(field_name)
        m2m_table_name = f"{singular_base}_{field_name}"

        if field_name in df.columns:
            m2m_df = (
                df.select(["id", field_name])
                .explode(field_name)
                .drop_nulls(subset=[field_name])
                .rename({
                    "id": f"{singular_base}_id",
                    field_name: f"{singular_rel}_id"
                })
                .unique()
            )
            if not m2m_df.is_empty():
                m2m_dfs[m2m_table_name] = m2m_df

    return m2m_dfs


def run_dq_checks(Model: type, df: pl.DataFrame) -> bool:
    """Executes data quality assertions on transformed DataFrame."""
    if df.is_empty():
        return True

    if "id" not in df.columns or df["id"].null_count() > 0:
        log_to_discord(f"DQ Check Failed: Null primary keys found in batch for {Model.__name__}", AlertLevel.ERROR)
        return False

    return True


def upsert_into_postgres(
    db_pool: ConnectionPool,
    schema: DatabaseSchema | str,
    table_name: str,
    df: pl.DataFrame,
    conflict_columns: list[str],
    is_m2m: bool = False
) -> None:
    """
    Idempotent database loading pattern:
    1. Writes DataFrame to a temporary staging table (_staging_<table_name>).
    2. Performs ON CONFLICT upsert from staging into the target table.
    3. Cleans up staging table in a finally block (always runs).
    """
    if df.is_empty():
        return

    schema_name = _validate_identifier(
        schema.value if isinstance(schema, DatabaseSchema) else schema, "schema"
    )
    table_name = _validate_identifier(table_name, "table")
    staging_table = f"_staging_{table_name}"

    # Write to staging table (replace ensures idempotency on retry)
    update_into_db(
        schema=schema,
        table=staging_table,
        df=df,
        if_table_exists="replace"
    )

    try:
        cols = [f'"{c}"' for c in df.columns]
        cols_str = ", ".join(cols)
        conflict_str = ", ".join(f'"{c}"' for c in conflict_columns)

        if is_m2m:
            on_conflict_clause = "DO NOTHING"
        else:
            update_assignments = [f'{c} = EXCLUDED.{c}' for c in cols if c.strip('"') not in conflict_columns]
            on_conflict_clause = (
                f"DO UPDATE SET {', '.join(update_assignments)}"
                if update_assignments
                else "DO NOTHING"
            )

        upsert_query = f"""
            INSERT INTO {schema_name}.{table_name} ({cols_str})
            SELECT {cols_str} FROM {schema_name}.{staging_table}
            ON CONFLICT ({conflict_str}) {on_conflict_clause};
        """
        execute_sql_from_string(pool=db_pool, query=upsert_query)

    finally:
        cleanup_query = f"DROP TABLE IF EXISTS {schema_name}.{staging_table};"
        execute_sql_from_string(pool=db_pool, query=cleanup_query)


def ingest_batches_to_postgres(
    azure_client: DataLakeServiceClient | BlobServiceClient, 
    db_pool: ConnectionPool
) -> None:
    """
    Reads newly landed raw/bronze batches from ADLS, transforms them with Polars,
    executes DQ checks, performs idempotent loading into Postgres ANALYTICS schema,
    and updates checkpoints.
    """
    run_id = str(uuid.uuid4())
    start_ingestion_run(pool=db_pool, run_id=run_id, layer="ANALYTICS")
    
    run_status: Literal["COMPLETED", "FAILED"] = "COMPLETED"
    run_error = None

    ensure_schema(db_pool=db_pool)

    data_to_ingest: dict[type, AnalyticsTask] = _get_watermark_dict(db_pool)

    for Model, task in data_to_ingest.items():
        table_name = Model.get_table_name()

        # 1. Ingestion from DataLake
        raw_df: pl.DataFrame = get_data_from_datalake(
            azure_client=azure_client,
            db_pool=db_pool,
            Model=Model,
            task=task
        )

        if raw_df.is_empty():
            continue

        aligned_df = enforce_schema_and_types(Model=Model, df=raw_df)
        clean_df = deduplicate_records(df=aligned_df)
        m2m_dfs = extract_m2m_relationships(Model=Model, df=clean_df)

        # Drop list columns from main table if non-SCD2 (since list cols go into M2M tables)
        if not Model._conserve_history:
            main_df_cols = [
                c for c in clean_df.columns
                if c in Model.model_fields  # guard first to prevent KeyError on tech columns
                and not _LIST_RE.match(Model._convert_to_polar_types(Model.model_fields[c].annotation))
            ] + ["_ingested_at", "_hash"]
            main_df = clean_df.select(main_df_cols)
        else:
            main_df = clean_df

        if not run_dq_checks(Model=Model, df=main_df):
            if task.is_fallback and task.event_id:
                update_fallback_event_status(db_pool, task.event_id, status="FAILED", error_message="DQ check failed")
            continue

        # Idempotent Postgres Loading (Main Table + Junction Tables)
        try:
            conflict_cols = ["id", "_valid_from"] if Model._conserve_history else ["id"]

            if Model._full_load:
                update_into_db(schema=DatabaseSchema.ANALYTICS, table=table_name, df=main_df, if_table_exists="replace")
            else:
                upsert_into_postgres(
                    db_pool=db_pool,
                    schema=DatabaseSchema.ANALYTICS,
                    table_name=table_name,
                    df=main_df,
                    conflict_columns=conflict_cols
                )

            for m2m_table, m2m_df in m2m_dfs.items():
                m2m_conflict = list(m2m_df.columns)
                upsert_into_postgres(
                    db_pool=db_pool,
                    schema=DatabaseSchema.ANALYTICS,
                    table_name=m2m_table,
                    df=m2m_df,
                    conflict_columns=m2m_conflict,
                    is_m2m=True
                )

        except Exception as load_err:
            err_msg = f"Load failed for {Model.__name__}: {load_err}"
            
            run_error = str(load_err)
            run_status = "FAILED"

            log_to_discord(err_msg, AlertLevel.ERROR)
            if task.is_fallback and task.event_id:
                update_fallback_event_status(db_pool, task.event_id, status="FAILED", error_message=err_msg)
            continue
        
        complete_ingestion_run(db_pool, run_id, run_status, run_error)

        # Checkpoints and Fallbacks
        if task.is_fallback and task.event_id:
            update_fallback_event_status(
                db_pool, 
                task.event_id, 
                status="COMPLETED", 
                records_processed=len(main_df)
            )
        else:
            raw_max = main_df["updated_at"].max() if "updated_at" in main_df.columns else None
            new_watermark: int = int(raw_max) if isinstance(raw_max, (int, float)) else task.start_watermark

            upsert_checkpoint(
                pool=db_pool,
                table_name=Model.__name__,
                current_watermark=new_watermark,
                last_id=0,
                layer="ANALYTICS",
                offset_val=0,
                run_id=None
            )
            upsert_fallback_checkpoint(
                pool=db_pool, 
                table_name=Model.__name__,
                layer='ANALYTICS', 
                fallback_watermark=new_watermark
            )