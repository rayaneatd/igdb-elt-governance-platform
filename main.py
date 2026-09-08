import os
import threading
from concurrent.futures import ThreadPoolExecutor

from src.datalake.service_client import init_datalake_service_client
from src.database.auth import init_database_engine
from src.utils.alerting import log_to_discord, AlertLevel 
from src.database import execute_sql_from_file, cleanup_orphan_runs

from src.handle_ingestion import do_ingestion
from src.database.analytics import consume_batches_to_postgres


def _run_raw_worker(client, pool, raw_finished_event: threading.Event) -> None:
    try:
        print("[PIPELINE] [THREAD-RAW] Executing RAW Ingestion layer...", flush=True)
        do_ingestion(client, pool)
        print("[PIPELINE] [THREAD-RAW] RAW Ingestion layer completed.", flush=True)
    finally:
        raw_finished_event.set()


def _run_analytics_worker(client, pool, raw_finished_event: threading.Event) -> None:
    print("[PIPELINE] [THREAD-ANALYTICS] Starting streaming ANALYTICS consumer...", flush=True)
    consume_batches_to_postgres(client, pool, stop_event=raw_finished_event, poll_interval=1.0)
    print("[PIPELINE] [THREAD-ANALYTICS] Streaming ANALYTICS consumer completed.", flush=True)


def run_full_pipeline(datalake_client=None, db_pool=None):                                                                                                                                                                                                   
    print("[PIPELINE] Starting Azure Market Insights ELT orchestration (Concurrent Mode)...", flush=True)

    client = datalake_client or init_datalake_service_client()
    pool = db_pool or init_database_engine()

    if client is None:
        log_to_discord("Error: Datalake service client not initialized", level=AlertLevel.ERROR)
        print("[PIPELINE ERROR] Datalake service client not initialized", flush=True)
        return 

    if pool is None:
        log_to_discord("Error: Database connection pool not initialized", level=AlertLevel.ERROR)
        print("[PIPELINE ERROR] Database connection pool not initialized", flush=True)
        return

    # Ensure log tables schema is applied before ingestion
    try:
        ddl_path = os.path.join(os.path.dirname(__file__), "src", "database", "models", "log_schemas.sql")
        execute_sql_from_file(pool, ddl_path)
    except Exception as e:
        log_to_discord(f"Critical error applying database migrations: {e}", level=AlertLevel.ERROR)
        print(f"[PIPELINE ERROR] Critical error applying migrations: {e}", flush=True)
        return
    
    # Clean up any dangling runs from previous crashed/interrupted sessions
    recovered = cleanup_orphan_runs(pool)
    if recovered > 0:
        print(f"[PIPELINE] Cleaned up {recovered} orphan run(s) left in 'RUNNING' state from a previous session.", flush=True)

    raw_finished_event = threading.Event()

    try:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="pipeline") as executor:
            raw_future = executor.submit(_run_raw_worker, client, pool, raw_finished_event)
            analytics_future = executor.submit(_run_analytics_worker, client, pool, raw_finished_event)

            raw_exc = raw_future.exception()
            analytics_exc = analytics_future.exception()

            if raw_exc:
                print(f"[PIPELINE ERROR] RAW Ingestion raised an exception: {raw_exc}", flush=True)
            if analytics_exc:
                print(f"[PIPELINE ERROR] ANALYTICS Consumer raised an exception: {analytics_exc}", flush=True)

            if raw_exc or analytics_exc:
                raise RuntimeError(f"Pipeline failure: raw_error={raw_exc}, analytics_error={analytics_exc}")

    except KeyboardInterrupt:
        print("\n[PIPELINE WARNING] Interrupted by user (Ctrl+C). Cleaning up active runs...", flush=True)
        raw_finished_event.set()
        cleanup_orphan_runs(pool, reason="Interrupted by user (Ctrl+C)")
        print("[PIPELINE] Active runs cleaned up successfully.", flush=True)
        raise

    print("[PIPELINE] ELT Pipeline execution completed successfully.", flush=True)

if __name__ == "__main__":
    run_full_pipeline()
