import time
import threading
from typing import Any
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

# Simple thread-safe in-memory TTL cache for heavy analytical aggregations
_CACHE_STORE: dict[str, tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()
DEFAULT_CACHE_TTL = 180.0  # 3 minutes

# ---------------------------------------------------------------------------
# Date sanity guard
# IGDB `first_release_date` is a Unix timestamp (seconds since epoch).
# Some entries carry:
#   - NULL          → game has no release date
#   - negative int  → relic / pre-1970 entry
#   - > 2051-01-01  → placeholder / far-future announced title
# We only display dates that convert to a calendar year in [1980, 2030].
# 1980-01-01 00:00:00 UTC  →  315_532_800
# 2031-01-01 00:00:00 UTC  →  1_924_991_999  (exclusive upper bound)
# ---------------------------------------------------------------------------
_DATE_GUARD_SQL = (
    "first_release_date IS NOT NULL "
    "AND first_release_date >= 315532800 "
    "AND first_release_date < 1924991999"
)


def _get_cached(key: str, ttl: float = DEFAULT_CACHE_TTL) -> Any | None:
    with _CACHE_LOCK:
        if key in _CACHE_STORE:
            ts, val = _CACHE_STORE[key]
            if time.time() - ts < ttl:
                return val
    return None


def _set_cached(key: str, val: Any) -> None:
    with _CACHE_LOCK:
        _CACHE_STORE[key] = (time.time(), val)


def clear_analytics_cache() -> None:
    with _CACHE_LOCK:
        _CACHE_STORE.clear()


def get_market_kpis(pool: ConnectionPool, force_refresh: bool = False) -> dict[str, Any]:
    """
    Retrieves high-level business intelligence KPIs across games, genres, and platforms.
    Cached for fast response.
    """
    cache_key = "market_kpis"
    if not force_refresh:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

    default_result = {
        "total_games": 0,
        "rated_games": 0,
        "avg_user_rating": None,
        "avg_critic_rating": None,
        "total_genres": 0,
        "total_platforms": 0,
        "top_release_year": None,
    }

    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # 1. Main games statistics
                cur.execute("""
                    SELECT 
                        COUNT(*) AS total_games,
                        COUNT(rating) AS rated_games,
                        ROUND(AVG(rating)::numeric, 1) AS avg_user_rating,
                        ROUND(AVG(aggregated_rating)::numeric, 1) AS avg_critic_rating
                    FROM public.games_scd2
                    WHERE _is_current = TRUE;
                """)
                row = cur.fetchone()
                if row:
                    default_result.update({
                        "total_games": int(row.get("total_games") or 0),
                        "rated_games": int(row.get("rated_games") or 0),
                        "avg_user_rating": float(row["avg_user_rating"]) if row.get("avg_user_rating") is not None else None,
                        "avg_critic_rating": float(row["avg_critic_rating"]) if row.get("avg_critic_rating") is not None else None,
                    })

                # 2. Total genres and platforms
                cur.execute("SELECT COUNT(*) AS cnt FROM public.genres;")
                g_row = cur.fetchone()
                if g_row:
                    default_result["total_genres"] = int(g_row.get("cnt") or 0)

                cur.execute("SELECT COUNT(*) AS cnt FROM public.platforms;")
                p_row = cur.fetchone()
                if p_row:
                    default_result["total_platforms"] = int(p_row.get("cnt") or 0)

                # 3. Peak release year (only validated timestamps)
                cur.execute(f"""
                    SELECT EXTRACT(YEAR FROM TO_TIMESTAMP(first_release_date))::INT AS yr, COUNT(*) AS cnt
                    FROM public.games_scd2
                    WHERE _is_current = TRUE AND {_DATE_GUARD_SQL}
                    GROUP BY yr
                    ORDER BY cnt DESC
                    LIMIT 1;
                """)
                yr_row = cur.fetchone()
                if yr_row and yr_row.get("yr"):
                    default_result["top_release_year"] = int(yr_row["yr"])

        _set_cached(cache_key, default_result)
        return default_result
    except Exception as e:
        return default_result


def get_genres_distribution(pool: ConnectionPool, limit: int = 10, force_refresh: bool = False) -> list[dict[str, Any]]:
    """
    Returns the distribution of games by genre.
    """
    cache_key = f"genres_distribution_{limit}"
    if not force_refresh:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

    query = """
        SELECT g.name AS genre_name, COUNT(gm.id) AS game_count
        FROM public.genres g
        JOIN public.games_scd2 gm ON g.id = ANY(gm.genres)
        WHERE gm._is_current = TRUE
        GROUP BY g.id, g.name
        ORDER BY game_count DESC
        LIMIT %s;
    """
    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (limit,))
                results = [dict(r) for r in cur.fetchall()]
                _set_cached(cache_key, results)
                return results
    except Exception:
        return []


def get_platforms_distribution(pool: ConnectionPool, limit: int = 10, force_refresh: bool = False) -> list[dict[str, Any]]:
    """
    Returns the top platforms by game volume.
    """
    cache_key = f"platforms_distribution_{limit}"
    if not force_refresh:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

    query = """
        SELECT p.name AS platform_name, COUNT(gm.id) AS game_count
        FROM public.platforms p
        JOIN public.games_scd2 gm ON p.id = ANY(gm.platforms)
        WHERE gm._is_current = TRUE
        GROUP BY p.id, p.name
        ORDER BY game_count DESC
        LIMIT %s;
    """
    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (limit,))
                results = [dict(r) for r in cur.fetchall()]
                _set_cached(cache_key, results)
                return results
    except Exception:
        return []


def get_releases_timeline(pool: ConnectionPool, force_refresh: bool = False) -> list[dict[str, Any]]:
    """
    Returns the historical trajectory of game releases per year from 1990 to 2026.
    """
    cache_key = "releases_timeline"
    if not force_refresh:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

    query = f"""
        SELECT EXTRACT(YEAR FROM TO_TIMESTAMP(first_release_date))::INT AS year,
               COUNT(*) AS games_count
        FROM public.games_scd2
        WHERE _is_current = TRUE
          AND {_DATE_GUARD_SQL}
        GROUP BY year
        ORDER BY year ASC;
    """
    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                results = [dict(r) for r in cur.fetchall()]
                _set_cached(cache_key, results)
                return results
    except Exception:
        return []


def get_top_rated_games(pool: ConnectionPool, limit: int = 10, min_ratings: int = 30) -> list[dict[str, Any]]:
    """
    Returns critically acclaimed games with a significant review sample size.
    """
    query = """
        SELECT id, name, slug, 
               ROUND(rating::numeric, 1) AS rating,
               rating_count,
               ROUND(total_rating::numeric, 1) AS total_rating,
               total_rating_count,
               hypes,
               EXTRACT(YEAR FROM TO_TIMESTAMP(first_release_date))::INT AS release_year,
               genres,
               platforms
        FROM public.games_scd2
        WHERE _is_current = TRUE 
          AND rating IS NOT NULL 
          AND rating_count >= %s
        ORDER BY rating DESC, rating_count DESC
        LIMIT %s;
    """
    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (min_ratings, limit))
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def get_lookup_maps(pool: ConnectionPool) -> tuple[dict[int, str], dict[int, str]]:
    """
    Returns id -> name maps for genres and platforms to format tabular results quickly.
    """
    genres_map = _get_cached("genres_map", ttl=600)
    platforms_map = _get_cached("platforms_map", ttl=600)

    if genres_map is not None and platforms_map is not None:
        return genres_map, platforms_map

    genres_map = {}
    platforms_map = {}

    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT id, name FROM public.genres;")
                for r in cur.fetchall():
                    genres_map[r["id"]] = r["name"]

                cur.execute("SELECT id, name FROM public.platforms;")
                for r in cur.fetchall():
                    platforms_map[r["id"]] = r["name"]

        _set_cached("genres_map", genres_map)
        _set_cached("platforms_map", platforms_map)
    except Exception:
        pass

    return genres_map, platforms_map


def search_games_explorer(
    pool: ConnectionPool,
    search: str | None = None,
    genre_id: int | None = None,
    sort_by: str = "rating",
    limit: int = 25,
    offset: int = 0
) -> dict[str, Any]:
    """
    Provides interactive exploration and filtering of indexed games.
    """
    genres_map, platforms_map = get_lookup_maps(pool)

    order_clause = "rating DESC NULLS LAST, rating_count DESC NULLS LAST"
    if sort_by == "recent":
        order_clause = "first_release_date DESC NULLS LAST"
    elif sort_by == "hypes":
        order_clause = "hypes DESC NULLS LAST"
    elif sort_by == "name":
        order_clause = "name ASC"

    # Apply the date guard only when sorting by release date or when a
    # year-derived column would otherwise surface NULL / outlier values.
    # For general browsing we keep all games but cap the displayed year.
    conditions = ["_is_current = TRUE"]
    params: list[Any] = []

    if search and search.strip():
        conditions.append("name ILIKE %s")
        params.append(f"%{search.strip()}%")

    if genre_id is not None:
        conditions.append("%s = ANY(genres)")
        params.append(genre_id)

    where_str = " AND ".join(conditions)

    count_query = f"SELECT COUNT(*) AS total FROM public.games_scd2 WHERE {where_str};"
    data_query = f"""
        SELECT id, name, slug,
               ROUND(rating::numeric, 1) AS rating,
               rating_count,
               hypes,
               -- Clamp to NULL when the timestamp is outside valid bounds so
               -- the UI never displays a nonsensical year (e.g. 2040, 1947).
               CASE
                   WHEN {_DATE_GUARD_SQL}
                   THEN EXTRACT(YEAR FROM TO_TIMESTAMP(first_release_date))::INT
                   ELSE NULL
               END AS release_year,
               genres,
               platforms
        FROM public.games_scd2
        WHERE {where_str}
        ORDER BY {order_clause}
        LIMIT %s OFFSET %s;
    """

    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(count_query, params)
                c_row = cur.fetchone()
                total_count = int(c_row["total"]) if c_row else 0

                cur.execute(data_query, params + [limit, offset])
                raw_rows = [dict(r) for r in cur.fetchall()]

                # Resolve human-readable genre and platform names
                for r in raw_rows:
                    g_ids = r.get("genres") or []
                    p_ids = r.get("platforms") or []
                    r["genre_names"] = [genres_map[gid] for gid in g_ids if gid in genres_map]
                    r["platform_names"] = [platforms_map[pid] for pid in p_ids if pid in platforms_map]

                return {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "items": raw_rows
                }
    except Exception:
        return {"total": 0, "limit": limit, "offset": offset, "items": []}
