import os
import sys
import logging
from dotenv import load_dotenv

# Ensure project root is in sys.path and environment is loaded
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from flask import Flask, jsonify, render_template, request, session
from src.database.auth import init_database_engine
from app.backend.auth import authenticate_user, login_required, admin_required
from app.backend.governance import (
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
from app.backend.dashboard import (
    get_market_kpis,
    get_genres_distribution,
    get_platforms_distribution,
    get_releases_timeline,
    get_top_rated_games,
    search_games_explorer,
    clear_analytics_cache,
)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Configure access logs to server.log file
log_file_path = os.path.join(BASE_DIR, "server.log")
logging.basicConfig(filename=log_file_path, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.setLevel(logging.ERROR)

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=BASE_DIR,
    static_url_path="",
)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "aero_governance_secret_key_2026_igdb")

# Connection pool to PostgreSQL
db_pool = init_database_engine()


@app.route("/")
def index():
    return render_template("index.html")


# ================================================================
# Auth APIs
# ================================================================

@app.route("/api/me", methods=["GET"])
def api_me():
    user = session.get("user")
    if user:
        return jsonify({"logged_in": True, "username": user["username"], "role": user["role"]})
    return jsonify({"logged_in": False, "username": None, "role": None})


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json() or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")

    user = authenticate_user(username, password)
    if not user:
        return jsonify({"error": "Invalid credentials. Please verify your username and password."}), 401

    session["user"] = {"username": user["username"], "role": user["role"]}
    return jsonify({"message": "Login successful", "username": user["username"], "role": user["role"]})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.pop("user", None)
    return jsonify({"message": "Logout successful"})


# ================================================================
# Market Analytics APIs (Protected by login_required)
# ================================================================

@app.route("/api/analytics/kpis", methods=["GET"])
@login_required
def api_analytics_kpis():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    kpis = get_market_kpis(db_pool, force_refresh=force_refresh)
    return jsonify(kpis)


@app.route("/api/analytics/genres", methods=["GET"])
@login_required
def api_analytics_genres():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    limit = request.args.get("limit", default=10, type=int)
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    data = get_genres_distribution(db_pool, limit=limit, force_refresh=force_refresh)
    return jsonify(data)


@app.route("/api/analytics/platforms", methods=["GET"])
@login_required
def api_analytics_platforms():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    limit = request.args.get("limit", default=10, type=int)
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    data = get_platforms_distribution(db_pool, limit=limit, force_refresh=force_refresh)
    return jsonify(data)


@app.route("/api/analytics/timeline", methods=["GET"])
@login_required
def api_analytics_timeline():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    data = get_releases_timeline(db_pool, force_refresh=force_refresh)
    return jsonify(data)


@app.route("/api/analytics/top-games", methods=["GET"])
@login_required
def api_analytics_top_games():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    limit = request.args.get("limit", default=10, type=int)
    min_ratings = request.args.get("min_ratings", default=30, type=int)
    data = get_top_rated_games(db_pool, limit=limit, min_ratings=min_ratings)
    return jsonify(data)


@app.route("/api/analytics/games", methods=["GET"])
@login_required
def api_analytics_games():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    search = request.args.get("search", default=None, type=str)
    genre_id = request.args.get("genre_id", default=None, type=int)
    sort_by = request.args.get("sort_by", default="rating", type=str)
    limit = request.args.get("limit", default=25, type=int)
    offset = request.args.get("offset", default=0, type=int)

    results = search_games_explorer(
        pool=db_pool,
        search=search,
        genre_id=genre_id,
        sort_by=sort_by,
        limit=limit,
        offset=offset
    )
    return jsonify(results)


# ================================================================
# Governance & ELT APIs (Protected by login_required)
# ================================================================

@app.route("/api/stats", methods=["GET"])
@login_required
def api_stats():
    if not db_pool:
        return jsonify({"error": "Database connection pool not initialized"}), 500
    stats = get_dashboard_stats(db_pool)
    return jsonify(stats)


@app.route("/api/runs", methods=["GET"])
@login_required
def api_runs():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500
    limit = request.args.get("limit", default=50, type=int)
    runs = get_runs_history(db_pool, limit=limit)
    return jsonify(runs)


@app.route("/api/checkpoints", methods=["GET"])
@login_required
def api_checkpoints():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500
    checkpoints = get_all_checkpoints(db_pool)
    return jsonify(checkpoints)


@app.route("/api/checkpoints/override", methods=["POST"])
@admin_required
def api_override_checkpoint():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500

    data = request.get_json() or {}
    table_name = data.get("table_name")
    custom_watermark = data.get("custom_watermark")
    activate_fallback = data.get("activate_fallback", True)

    if not table_name:
        return jsonify({"error": "Missing required field: table_name"}), 400

    if custom_watermark is not None:
        try:
            custom_watermark = int(custom_watermark)
        except ValueError:
            return jsonify({"error": "Invalid format for custom_watermark"}), 400

    success = set_checkpoint_override(
        pool=db_pool,
        table_name=table_name,
        custom_watermark=custom_watermark,
        activate_fallback=activate_fallback,
    )

    if success:
        return jsonify({
            "message": f"Fallback successfully activated for {table_name}",
            "table_name": table_name,
            "is_override_active": activate_fallback,
            "custom_watermark": custom_watermark,
        })
    else:
        return jsonify({"error": f"Table {table_name} not found in checkpoints"}), 404


@app.route("/api/fallback-events", methods=["GET"])
@login_required
def api_get_fallback_events():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500
    events = get_fallback_events(db_pool)
    return jsonify(events)


@app.route("/api/fallback-events", methods=["POST"])
@admin_required
def api_create_fallback_event():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500

    data = request.get_json() or {}
    table_name = data.get("table_name")
    start_watermark = data.get("start_watermark")
    end_watermark = data.get("end_watermark")

    if not table_name or start_watermark is None or end_watermark is None:
        return jsonify({"error": "Missing required fields: table_name, start_watermark, end_watermark"}), 400

    try:
        start_watermark = int(start_watermark)
        end_watermark = int(end_watermark)
    except ValueError:
        return jsonify({"error": "Invalid format for start_watermark / end_watermark timestamps"}), 400

    event = create_fallback_event(db_pool, table_name, start_watermark, end_watermark)
    return jsonify({"message": "Fallback event successfully created", "event": event})


@app.route("/api/batches", methods=["GET"])
@login_required
def api_batches():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500
    limit = request.args.get("limit", default=100, type=int)
    table_name = request.args.get("table_name", default=None, type=str)
    batches = get_recent_batches(db_pool, limit=limit, table_name=table_name)
    return jsonify(batches)


@app.route("/api/schema-history", methods=["GET"])
@login_required
def api_schema_history():
    if not db_pool:
        return jsonify({"error": "Database pool unavailable"}), 500
    drifts = get_schema_drifts(db_pool)
    return jsonify(drifts)


@app.route("/api/tables", methods=["GET"])
@login_required
def api_tables():
    return jsonify({"tables": get_registered_tables()})


if __name__ == "__main__":
    host = os.environ.get("FLASK_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_PORT", 5000))
    app.run(host=host, port=port, debug=True)
