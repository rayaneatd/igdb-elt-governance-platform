from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True, frozen=True)
class TableCheckpoint:
    """Represents an ingestion checkpoint for a table."""
    table_name: str
    current_watermark: int
    fallback_watermark: int
    last_id: int
    offset_val: int
    is_override_active: bool
    last_batch_id: int = 0


@dataclass(slots=True, frozen=True)
class FallbackEvent:
    """Represents a pending fallback event for reprocessing past date ranges."""
    event_id: str
    table_name: str
    layer: str
    start_watermark: int
    end_watermark: Optional[int]
    status: str


@dataclass(slots=True)
class AnalyticsTask:
    """Represents resolved analytics processing state for a table."""
    start_watermark: int
    end_watermark: Optional[int]
    is_fallback: bool
    event_id: Optional[str]
    last_batch_id: int = 0


@dataclass(slots=True, frozen=True)
class RawBatchRef:
    """Represents an unconsumed raw batch file reference from logs.batch_logs."""
    batch_id: int
    path: str
    cursor_value: int
    offset_value: int
    created_at: datetime

