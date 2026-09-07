# Azure Market Insights — IGDB ELT & Governance Platform

[![Python 3.13+](https://img.shields.io/badge/Python-3.13+-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Data_Engine-Polars-CD792C.svg?logo=polars&logoColor=white)](https://pola.rs/)
[![Azure Data Lake Gen2](https://img.shields.io/badge/Storage-Azure_ADLS_Gen2-0078D4.svg?logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/products/storage/data-lake-storage/)
[![PostgreSQL 16](https://img.shields.io/badge/Database-PostgreSQL_16-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Flask](https://img.shields.io/badge/Control_Plane-Flask-000000.svg?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![uv](https://img.shields.io/badge/Package_Manager-uv-DE5FE9.svg?logo=astral&logoColor=white)](https://docs.astral.sh/uv/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-grade ELT (Extract-Load-Transform) pipeline and real-time governance platform that ingests video game market intelligence from the [IGDB API](https://api-docs.igdb.com/) into **Azure Data Lake Storage Gen2** (Bronze/Raw) and **PostgreSQL** (Analytics Layer), featuring adaptive schema drift detection, an event-driven FIFO fallback engine, and a **Frutiger Aero** governance dashboard.

---

## 🏛 Architecture Overview

![Architecture Overview](docs/architecture.png)

---

## ✨ Key Technical Highlights

### 1. Unified Schemas as Single Source of Truth (SSOT)
Each IGDB entity is modeled as a [Patito](https://github.com/Jakob-A-K/patito) model inheriting from [`BaseIGDBSchema`](file:///c:/Users/Administrator/Documents/PROJECT%20DATA/azure-market-insights-elt/src/igdb/models/BASE.py) (combining Pydantic v2 validation and Polars schema typing):
- **Apicalypse Query Generation**: Automatically builds dynamic API queries (`fields`, `where`, `sort`, `limit`, `offset`).
- **DDL & Index Synthesis**: Generates PostgreSQL table creation DDL, primary/foreign keys, and indexes on demand.
- **Dynamic Index Synchronization**: Compares existing PostgreSQL catalog indexes against model definitions, creating missing indexes and dropping obsolete ones deterministically without touching constraints.

### 2. Event-Driven FIFO Fallback Replays
Replaying historical data windows never mutates live incremental checkpoints:
- Fallback requests are queued as **isolated events** in `logs.fallback_events`.
- Ingestion processes pending fallback windows first (`start_watermark` → `end_watermark`), then cleanly resumes continuous incremental watermarks.
- If a replay fails, the incremental position remains 100% intact.

### 3. Adaptive Schema Drift Detection
When IGDB introduces new fields or alters signatures:
- The ingestion continues uninterrupted (`extra = 'ignore'`).
- The schema difference is detected via cryptographic hashing and logged into `logs.schema_history` as structured `JSONB` (`added`, `removed`, `type_changed`).
- Discord webhook alerts notify data engineers without crashing batch workloads.

### 4. Zero-Pandas High Performance Data Flow
- Raw ingestion persists immutable JSON into Azure Data Lake (or local Azurite emulator).
- The Analytics pipeline downloads batches using a `ThreadPoolExecutor` and transforms them with [Polars](https://pola.rs/) using Rust-backed vectorization.
- Writes to PostgreSQL utilize the **Apache Arrow ADBC** driver (`adbc-driver-postgresql`) through staging tables, guaranteeing atomic, idempotent `ON CONFLICT` upserts.

### 5. Frutiger Aero Governance Control Plane
- Lightweight, responsive web interface built with Flask and styled using a modern **Frutiger Aero** aesthetic (glassmorphism, radial glow gradients, live status indicators).
- **Role-Based Access Control (RBAC)**: `ADMIN` can trigger manual/targeted fallbacks and edit checkpoints; `VIEWER` gets read-only access.
- **Lazy Tab-Aware Polling**: Queries only the data required for the active tab, reducing database load by over 80%.
- Real-time telemetry: Ingestion runs, batch logs with full Apicalypse queries, checkpoint frontiers, and schema drift logs.

---

## 📂 Project Structure

```
.
├── main.py                          # Pipeline entry point (orchestrates Raw + Analytics)
├── deploy.ps1                       # Windows PowerShell deployment & automation script
├── deploy.sh                        # Linux / macOS / WSL deployment script
├── docker-compose.yml               # Local infrastructure (PostgreSQL 16 + Azurite)
├── example.env                      # Configuration template with documented variables
├── pyproject.toml                   # Project dependencies and packaging (uv)
│
├── src/
│   ├── config.py                    # Environment resolution & credentials loading
│   ├── handle_ingestion.py          # RAW layer ingestion engine (API -> ADLS)
│   │
│   ├── database/                    # Database & Analytics module
│   │   ├── analytics.py             # Polars transformations, DQ checks & Postgres upsert
│   │   ├── auth.py                  # Psycopg 3 connection pool initialization
│   │   ├── core.py                  # ADBC write path, raw SQL execution & validation
│   │   ├── fallback.py              # Event-driven fallback queries & state tracking
│   │   ├── logs.py                  # Audit logger (runs, batches, checkpoints, schema)
│   │   ├── types.py                 # Dataclasses & schema structs
│   │   └── models/
│   │       └── log_schemas.sql      # DDL for all logs and governance tables
│   │
│   ├── datalake/                    # Azure Data Lake Storage Gen2 module
│   │   ├── functions.py             # Dual-mode read/write (Azurite Blob / ADLS Gen2 DFS)
│   │   └── service_client.py        # Client authentication (DefaultAzureCredential / Azurite)
│   │
│   ├── igdb/                        # IGDB Client & Models
│   │   ├── auth.py                  # Twitch OAuth2 client credentials flow & token cache
│   │   ├── client.py                # HTTPX client with Tenacity retry & rate-limit handling
│   │   ├── rate_limit.py            # Thread-safe Token Bucket rate limiter (4 req/s)
│   │   └── models/                  # SSOT Data Models (Patito / Pydantic v2)
│   │       ├── BASE.py              # BaseIGDBSchema abstract base class
│   │       ├── games.py             # GameSchema (SCD2 history tracking)
│   │       ├── companies.py         # CompanySchema
│   │       ├── genres.py            # GenreSchema
│   │       ├── platforms.py         # PlatformSchema
│   │       └── release_dates.py     # ReleaseDateSchema
│   │
│   └── utils/
│       ├── alerting.py              # Discord webhook alerting
│       └── types.py                 # Python -> Polars -> PostgreSQL type mappings
│
├── app/                             # Frutiger Aero Governance Dashboard
│   ├── server.py                    # Flask server, authentication & REST APIs
│   ├── style.css                    # Frutiger Aero design system tokens & animations
│   ├── templates/
│   │   └── index.html               # SPA frontend (KPIs, Runs, Checkpoints, Batch Logs)
│   └── backend/
│       └── functions.py             # Dashboard queries, password hashing & drift parsing
│
└── tests/
    └── public/                      # Unit & integration test suite
        ├── test_analytics_pipeline.py
        ├── test_igdb_models.py
        └── test_index_management.py
```

---

## 🚀 Quick Start Guide

### Prerequisites
- [Python 3.13+](https://www.python.org/)
- [`uv`](https://docs.astral.sh/uv/) (recommended) or `pip`
- [Docker Desktop](https://www.docker.com/) (for local PostgreSQL & Azurite)
- A free [Twitch Developer Application](https://dev.twitch.tv/console/apps) for IGDB Client ID & Secret

---

### ⚙️ Initial Configuration (One-time)

Before launching, configure your environment variables:
```bash
cp example.env .env
```
Open `.env` and fill in your Twitch credentials:
```env
TWITCH_CLIENT_ID=your_client_id_here
TWITCH_CLIENT_SECRET=your_client_secret_here
```
*(All other settings in `.env` are preconfigured with local Docker defaults for PostgreSQL and Azurite).*

---

### 🌟 Deployment Modes

Choose the deployment workflow that fits your needs:

| Mode | Command (Windows) | Command (Linux / macOS) | Best For |
|---|---|---|---|
| **Mode 1: All-in-One** *(Recommended)* | `.\deploy.ps1 all` (or `.\deploy.ps1`) | `./deploy.sh all` (or `./deploy.sh`) | First run & quick start: sets up everything, runs the pipeline, and auto-opens the web dashboard. |
| **Mode 2: Modular / Step-by-Step** | `.\deploy.ps1 [action]` | `./deploy.sh [action]` | Development & debugging: run setup, pipeline batches, tests, or dashboard independently. |
| **Mode 3: Continuous Ingestion** | `.\deploy.ps1 schedule -IntervalMinutes 15` | `./deploy.sh schedule 15` | Automated operations: runs the pipeline in a continuous loop every N minutes. |
| **Mode 4: Manual (No Scripts)** | Standard CLI commands | Standard CLI commands | Production containers or environments without PowerShell/Bash scripts. |

---

### Mode 1 — All-in-One Automated Deployment (Recommended)

This single command handles the entire end-to-end lifecycle without requiring any manual intervention:

#### On Windows (PowerShell):
```powershell
.\deploy.ps1
# Or explicitly:
.\deploy.ps1 all
```

#### On Linux / macOS / WSL (Bash):
```bash
chmod +x deploy.sh
./deploy.sh
# Or explicitly:
./deploy.sh all
```

#### 🔍 What happens automatically under the hood:
1. **Prerequisites Check**: Verifies that Python 3.13+, `uv`, and Docker are installed and available in PATH.
2. **Environment File**: Ensures `.env` is present (creates it from `example.env` if missing).
3. **Local Infrastructure**: Starts Docker Compose services (`igdb-elt-postgres` on port `5432` and `igdb-elt-azurite` on ports `10000`/`10001`).
4. **Dependencies**: Synchronizes all Python packages in seconds via `uv sync`.
5. **Database Migrations**: Automatically applies `src/database/models/log_schemas.sql` to initialize all governance and audit tables.
6. **Unit Tests**: Runs the test suite (`tests/public`) to validate schema constraints, models, and index synchronization.
7. **ELT Pipeline Execution**: Runs `main.py`, which checks checkpoints, queries IGDB with token bucket rate-limiting (4 req/s), saves raw JSON into Azurite Bronze, vectorizes and transforms the data with Polars, and performs atomic upserts into PostgreSQL.
8. **Browser Auto-Launch**: Automatically opens your default web browser to `http://localhost:5000`.
9. **Dashboard Launch**: Starts the Flask Frutiger Aero governance server at `http://localhost:5000`.

---

### Mode 2 — Modular / Step-by-Step Deployment

If you want granular control over individual services during development:

```powershell
# 1. Setup environment only (containers, dependencies, migrations, tests):
.\deploy.ps1 setup

# 2. Run a single ELT batch (API -> Azurite -> Polars -> PostgreSQL):
.\deploy.ps1 pipeline

# 3. Start only the Governance Dashboard (auto-opens browser):
.\deploy.ps1 app

# 4. Run the unit test suite:
.\deploy.ps1 test

# 5. Start or stop local infrastructure containers:
.\deploy.ps1 up      # Start Postgres & Azurite
.\deploy.ps1 down    # Stop containers

# 6. Check environment integrity:
.\deploy.ps1 check
```

*(On Linux / macOS, replace `.\deploy.ps1 <action>` with `./deploy.sh <action>`)*

---

### Mode 3 — Continuous Scheduled Ingestion (Hands-Free)

To keep your analytics database synchronized with IGDB in real time without manually triggering batches, run the pipeline in continuous scheduled mode:

```powershell
# Runs every 15 minutes (default):
.\deploy.ps1 schedule

# Or specify a custom interval (e.g., every 30 minutes):
.\deploy.ps1 schedule -IntervalMinutes 30
```

*(On Linux / macOS: `./deploy.sh schedule 30`)*

The scheduler runs an immediate batch, waits for the configured interval, and continuously queries subsequent delta windows using updated watermark timestamps.

---

### Mode 4 — Manual Step-by-Step (Without Scripts)

For CI/CD pipelines, production servers, or developers preferring direct CLI commands:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/rayaneatd/azure-market-insights-elt.git
   cd azure-market-insights-elt
   ```

2. **Configure Environment:**
   ```bash
   cp example.env .env
   # Edit .env with your TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET
   ```

3. **Install Dependencies:**
   ```bash
   uv sync
   ```

4. **Start Local Infrastructure (PostgreSQL 16 + Azurite):**
   ```bash
   docker compose up -d
   ```

5. **Apply Database Migrations:**
   ```bash
   uv run python -c "from src.database.auth import init_database_engine; from src.database.core import execute_sql_from_file; execute_sql_from_file(init_database_engine(), 'src/database/models/log_schemas.sql'); print('Migrations applied!')"
   ```

6. **Run the ELT Pipeline:**
   ```bash
   uv run python main.py
   ```

7. **Start the Governance Dashboard:**
   ```bash
   uv run python app/server.py
   # → Open http://localhost:5000 in your browser
   ```

---

## 🔒 Governance Dashboard & Access

The dashboard includes full-screen authentication protection and role-based access control:

| Role | Default Username | Default Password | Capabilities |
|---|---|---|---|
| **Administrator** | `admin` | `admin123` (or via `ADMIN_PASSWORD` in `.env`) | Full control: Trigger fallbacks, alter watermarks, inspect logs & schema |
| **Viewer** | `visitor` | `visitor123` (or via `VIEWER_PASSWORD` in `.env`) | Read-only: View KPIs, runs, batch logs, checkpoints and schema audits |

---

## 🧪 Testing & Validation

Execute the test suite with `uv` or Python `unittest`:

```powershell
# Via deploy script
.\deploy.ps1 test

# Directly via unittest
uv run python -m unittest discover tests/public
```

Test coverage includes:
- Polars DataFrame schema enforcement & unmapped field discarding.
- Primary key deduplication keeping latest record timestamps.
- Relationship explosion into junction tables (M2M join tables).
- Apicalypse query builder validation & filter formatting.
- Dynamic index generation and 63-byte PostgreSQL NAMEDATALEN limit safety.
- Dynamic index synchronization (selective `DROP` and `CREATE INDEX`).

---

## 🗺 Project Roadmap

- [x] **Raw Ingestion Layer** — Throttled extraction to ADLS Bronze JSON.
- [x] **Event-Driven Fallbacks** — FIFO queue decoupled from continuous checkpoints.
- [x] **Analytics Layer** — High-performance Polars transformation and ADBC PostgreSQL upsert.
- [x] **Schema Drift Auditing** — Cryptographic signature hashing and JSONB column tracking.
- [x] **Frutiger Aero Dashboard** — Real-time control plane with RBAC and lazy tab-aware polling.
- [x] **One-Click Deployment** — Cross-platform scripts (`deploy.ps1`, `deploy.sh`) and Docker Compose.
- [ ] **Azure Container Apps** — Cloud container deployment with Managed Identity.
- [ ] **Orchestration with Airflow or Dagster or Prefect** — Scheduled cron DAGs and alerting integration.

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
