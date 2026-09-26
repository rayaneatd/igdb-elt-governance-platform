"""
Backward-compatibility facade for app.backend.functions.
All implementation is modularized into auth.py, governance.py, and analytics.py.
"""

from .auth import authenticate_user, authenticate_user as authenticate_user_hardcoded, login_required, admin_required
from .governance import (
    get_registered_tables,
    get_dashboard_stats,
    get_runs_history,
    get_all_checkpoints,
    set_checkpoint_override,
    get_recent_batches,
    get_schema_drifts,
    create_fallback_event,
    get_fallback_events,
)
from .analytics import (
    get_market_kpis,
    get_genres_distribution,
    get_platforms_distribution,
    get_releases_timeline,
    get_top_rated_games,
    search_games_explorer,
    clear_analytics_cache,
)

__all__ = [
    "authenticate_user",
    "authenticate_user_hardcoded",
    "login_required",
    "admin_required",
    "get_registered_tables",
    "get_dashboard_stats",
    "get_runs_history",
    "get_all_checkpoints",
    "set_checkpoint_override",
    "get_recent_batches",
    "get_schema_drifts",
    "create_fallback_event",
    "get_fallback_events",
    "get_market_kpis",
    "get_genres_distribution",
    "get_platforms_distribution",
    "get_releases_timeline",
    "get_top_rated_games",
    "search_games_explorer",
    "clear_analytics_cache",
]
