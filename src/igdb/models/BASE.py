# 1. stdlib
import types
from typing import (
    ClassVar, 
    Literal,
    Sequence,
    Union,
    get_args,
    get_origin
)
from hashlib import sha256

IndexElement = str | Sequence[str]

# 2. third party
import patito as pt
import msgspec
from pydantic import ConfigDict

# 3. local
from src.utils.types import (
    PY_TO_PL_STR,
    PY_TO_SQL   ,
    _LIST_RE
)


# ================================================================
# 1. CONFIG
# ================================================================

# we can modify the version of the API by changing this variable
#! NEVER PUT A SLASH AT THE END OF THE URL
BASE_IGDB_URL = "https://api.igdb.com/v4"

# 1577836800 means we start in the year 2020
STARTING_TIMESTAMP_IGDB_TABLES = 1577836800  

MODEL_CONFIG = ConfigDict(
    extra            = 'ignore',
    populate_by_name = True    
)


# ================================================================
# 2. TECHNICAL COLUMNS - for postgres only
# ================================================================

TECH_COLUMNS = {
    "_ingested_at": "TIMESTAMPTZ DEFAULT NOW() NOT NULL",
    "_hash": "TEXT NOT NULL"
}

TECH_SCD2 = TECH_COLUMNS | {
    "_valid_from": "TIMESTAMPTZ DEFAULT NOW() NOT NULL",
    "_valid_to": "TIMESTAMPTZ",
    "_is_current": "BOOLEAN DEFAULT TRUE NOT NULL",
}

#TODO: auto indexation
#TODO: on devrait aussi ajouter un checkpoint pour chaque table créée

# ================================================================
# 3. BASE SCHEMA - basically the mother of classes
# ================================================================

class BaseIGDBSchema(pt.Model):
    """
    Base class for all IGDB schemas.
    Inherits from patito.Model (which itself inherits from Pydantic BaseModel),
    enabling both Pydantic validation at ingestion time and Polars DataFrame
    validation at the transformation step via .validate() / .from_dataframe().

    ClassVars are pipeline metadata (endpoint path, pagination params, etc.)
    and are NOT treated as model fields by Pydantic.
    """

    # ==========================================================
    # VARIABLES 
    # ==========================================================

        # propreties        
    _endpoint:         ClassVar[str]
    _starting_point:   ClassVar[int]         = STARTING_TIMESTAMP_IGDB_TABLES
    _limit:            ClassVar[int]         = 500
    _offset:           ClassVar[int]         = 0
    _conserve_history: ClassVar[bool]        = False
    _index_at:         ClassVar[Sequence[IndexElement] | str] = ()
    _tables:           ClassVar[dict[str, int]] = {}
    _full_load:        ClassVar[bool]        = False

        # config
    model_config = MODEL_CONFIG
    


    # ==========================================================
    # METHODS 
    # ==========================================================

    # private
    @staticmethod
    def _to_singular(name: str) -> str:
        """
        transforms a pleural word to singular
        """
        
        # supports snake_case: involved_companies -> involved_company
        parts = name.split('_')
        last = parts[-1]

        if last.endswith('ies'):
            last = last[:-3] + 'y'  # companies -> company
        elif last.endswith('ses') or last.endswith('xes') or last.endswith('ches') or last.endswith('shes'):
            last = last[:-2]         # statuses -> status, genres -> genre? no

            # cas genres = genre + s, on le rattrape en dessous
            if last.endswith('e') and name.endswith('es'):
                pass
        elif last.endswith('s') and not last.endswith('ss'):
            last = last[:-1]         # genres -> genre, platforms -> platform

        parts[-1] = last
        return '_'.join(parts)

    @staticmethod # convert python to polar types
    def _convert_to_polar_types(t) -> str:
        """
        Convert Python annotation to Polars type string.
        Unwraps Optional[T], handles List[T] to List(PolType), etc.
        """
        origin = get_origin(t)
        if origin is Union or origin is types.UnionType:
            args = [a for a in get_args(t) if a is not type(None)]
            if args:
                t = args[0]
                origin = get_origin(t)

        if origin is list:
            inner = get_args(t)[0]
            # au cas où c'est list[str | None]
            inner_origin = get_origin(inner)
            if inner_origin is Union or inner_origin is types.UnionType:
                inner_args = [a for a in get_args(inner) if a is not type(None)]
                if inner_args:
                    inner = inner_args[0]
            return f"List({PY_TO_PL_STR.get(inner, getattr(inner, '__name__', str(inner)))})"

        return PY_TO_PL_STR.get(t, getattr(t, "__name__", str(t)))
    
    @classmethod # convert polar to postgres types
    def _pl_to_pg(cls, pl_type: str, use_arrays: bool = True) -> str | None:
        """
        Convert a Polars type string (output of _convert_to_polar_types)
        to a Postgres type.

        - List(Int64) -> BIGINT[] if use_arrays=True
        - List(Int64) -> None if use_arrays=False (to create M2M table)
        """
        pl_type = pl_type.strip()

        # 1. in case it's a List
        match = _LIST_RE.match(pl_type)
        if match:
            inner_pl = match.group(1).strip()
            inner_pg = cls._pl_to_pg(inner_pl, use_arrays=use_arrays)
            if inner_pg is None:
                return None
            if not use_arrays:
                return None  # signal to create a join table

            # e.g: Int64 -> BIGINT -> BIGINT[]
            return f"{inner_pg}[]"

        # 2. dates
        if pl_type.startswith("Datetime"):
            return "TIMESTAMPTZ"
        if pl_type.startswith("Duration"):
            return "INTERVAL"

        # 3. simple case
        return PY_TO_SQL.get(pl_type, "TEXT")
    
    @classmethod #TODO convert constraints
    def _convert_constraints(cls):
        pass

    @classmethod # get dict of columns
    def _get_columns_snapshot_dict(cls, is_clean):
        return {
            name: cls._convert_to_polar_types(info.annotation) if is_clean else str(info.annotation)
            for name, info in cls.model_fields.items()
        }

    @classmethod # get technical columns
    def _get_tech_columns(cls):
        return TECH_SCD2 if cls._conserve_history else TECH_COLUMNS

    # public
    @classmethod # get apicalypse fields
    def apicalypse_fields(cls) -> str:
        """
        Generates a comma-separated string of all declared fields for the IGDB
        Apicalypse query. Uses field alias if defined, skips ClassVar metadata.
        """
        return ", ".join(
            info.alias or name
            for name, info in cls.model_fields.items()
        )
        
    @classmethod # build apicalypse query
    def build_query(
        cls,
        last_update_value: int = 0,
        filters: str = "",
        sort: str = "updated_at asc",  #! MULTI SORTING IS NOT SUPPORTED BY IGDB API
        limit: int = 500,
        offset: int = 0
    ) -> str:
        """
        Constructs a complete Apicalypse query string for the IGDB API.

        Parameters:
        - last_update_value: Unix timestamp to fetch only records updated after this date.
        - filters: Additional custom Apicalypse filter conditions (e.g., "platforms = (48)").
        - sort: Sorting criteria (defaults to ascending order of update time).
        - limit: Maximum number of records to return (capped at 500).
        - offset: Number of records to skip for pagination.
        """
        query_parts = [f"fields {cls.apicalypse_fields()};"]
        where_conditions = []

        if cls._full_load:
            last_update_value = 0

        if last_update_value:
            where_conditions.append(f"updated_at >= {last_update_value}")

        if filters:
            where_conditions.append(f"({filters})")

        if where_conditions:
            query_parts.append(f"where {' & '.join(where_conditions)};")

        if sort:
            query_parts.append(f"sort {sort};")

        query_parts.append(f"limit {min(limit, 500)};")

        if offset:
            query_parts.append(f"offset {offset};")

        return " ".join(query_parts)

    @classmethod
    def get_table_name(cls) -> str:
        """Returns the PostgreSQL table name for this model schema."""
        base_name = cls._endpoint.strip('/').replace('/', '_')
        return f"{base_name}_scd2" if cls._conserve_history else base_name

    @classmethod # build query
    def build_pg_query(cls):
        base_name = cls._endpoint.strip('/').replace('/', '_')
        singular_base = cls._to_singular(base_name)
        table_name = cls.get_table_name()
        use_arrays = cls._conserve_history # the use of arrays is mandatory for scd2

        main_cols = []
        m2m_tables_dict = {} 
        tech_cols = cls._get_tech_columns()
        
        for name, info in cls.model_fields.items():
            pl_type = cls._convert_to_polar_types(info.annotation)
            pg_type = cls._pl_to_pg(pl_type, use_arrays=use_arrays)
            
            if pg_type is None:
                inner_pl = _LIST_RE.match(pl_type).group(1) # pyrefly: ignore[missing-attribute]
                inner_pg = cls._pl_to_pg(inner_pl)
                singular_rel = cls._to_singular(name)
                m2m_table_name = f"{singular_base}_{name}_scd2" if cls._conserve_history else f"{singular_base}_{name}"

                # avoid duplication
                if m2m_table_name in m2m_tables_dict:
                    continue 

                if cls._conserve_history:
                    
                    cols = {
                        f"{singular_base}_id": "BIGINT NOT NULL",
                        f"{singular_rel}_id": f"{inner_pg} NOT NULL",
                    }
                    cols.update(tech_cols)
                    
                    col_defs = [f'"{k}" {v}' if k in tech_cols else f"{k} {v}" for k,v in cols.items()]
                    col_defs.append(f'PRIMARY KEY ({singular_base}_id, {singular_rel}_id, "_valid_from")')

                    m2m_tables_dict[m2m_table_name] = [
                        f"CREATE TABLE IF NOT EXISTS {m2m_table_name} (\n  " + ",\n  ".join(col_defs) + "\n);",
                        f"CREATE INDEX IF NOT EXISTS idx_{m2m_table_name}_{singular_rel}_id ON {m2m_table_name} ({singular_rel}_id);",
                        f"CREATE INDEX IF NOT EXISTS idx_{m2m_table_name}_current ON {m2m_table_name} ({singular_base}_id) WHERE _is_current = TRUE;"
                    ]
                else:
                    m2m_tables_dict[m2m_table_name] = [
                        f"CREATE TABLE IF NOT EXISTS {m2m_table_name} (\n"
                        f"  {singular_base}_id BIGINT REFERENCES {table_name}(id),\n"
                        f"  {singular_rel}_id {inner_pg},\n"
                        f"  PRIMARY KEY ({singular_base}_id, {singular_rel}_id)\n);",
                        f"CREATE INDEX IF NOT EXISTS idx_{m2m_table_name}_{singular_rel}_id ON {m2m_table_name} ({singular_rel}_id);"
                    ]
                continue

            col_def = f'"{name}" {pg_type}'
            if info.is_required():
                col_def += " NOT NULL"
            if not cls._conserve_history and (getattr(info, 'primary_key', False) or name == "id"):
                col_def += " PRIMARY KEY"
            if getattr(info, 'unique', False):
                col_def += " UNIQUE"
            main_cols.append(col_def)

        for field, definition in tech_cols.items():
            main_cols.append(f'"{field}" {definition}')
        if cls._conserve_history:
            main_cols.append('PRIMARY KEY ("id", "_valid_from")')

        main_ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  " + ",\n  ".join(main_cols) + "\n);"
        
        indexes = []
        for cols in cls.get_indexes():
            idx_name = cls.get_index_name(cols)
            cols_str = ", ".join(f'"{c}"' for c in cols)
            indexes.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({cols_str});")

        m2m_tables = [ddl for lst in m2m_tables_dict.values() for ddl in lst]
        return "\n\n".join([main_ddl] + indexes + m2m_tables)

    @classmethod
    def get_indexes(cls) -> list[tuple[str, ...]]:
        """
        Normalizes and validates index specifications declared in cls._index_at.

        Supported input formats:
        - Single column string: "name" -> [("name",)]
        - Single column tuple/list: ("name",) -> [("name",)]
        - Multiple single-column indexes: ("name", "slug") -> [("name",), ("slug",)]
        - Composite indexes: (("game_status", "rating"), "name") -> [("game_status", "rating"), ("name",)]

        Returns:
            A list of column tuples representing each index to create.

        Raises:
            ValueError: If an index element is invalid or references an unmapped column.
        """
        raw_indexes = getattr(cls, "_index_at", ())
        if not raw_indexes:
            return []

        if isinstance(raw_indexes, str):
            raw_list = [raw_indexes]
        elif isinstance(raw_indexes, (list, tuple)):
            raw_list = list(raw_indexes)
        else:
            raise ValueError(
                f"Invalid _index_at specification in {cls.__name__}: expected str or sequence, got {type(raw_indexes).__name__}"
            )

        valid_fields = set(cls.model_fields.keys()) | set(cls._get_tech_columns().keys()) | {"id"}
        normalized: list[tuple[str, ...]] = []

        for item in raw_list:
            if isinstance(item, str):
                cols = (item,)
            elif isinstance(item, (list, tuple)):
                if not item:
                    continue
                cols = tuple(str(c) for c in item)
            else:
                raise ValueError(
                    f"Invalid index element {item!r} in {cls.__name__}._index_at: expected str or sequence of str"
                )

            for col in cols:
                if col not in valid_fields:
                    raise ValueError(
                        f"Column {col!r} specified in {cls.__name__}._index_at does not exist in schema fields or technical columns."
                    )

            normalized.append(cols)

        return normalized

    @classmethod
    def get_index_name(cls, cols: tuple[str, ...]) -> str:
        """
        Deterministically builds a valid PostgreSQL index name for the specified columns.

        PostgreSQL limits identifier length to 63 bytes (NAMEDATALEN - 1).
        If the identifier exceeds 63 characters, it is truncated and appended
        with a deterministic SHA256 digest to prevent collision and overflow.
        """
        table_name = cls.get_table_name()
        cols_slug = "_".join(cols)
        full_name = f"idx_{table_name}_{cols_slug}"
        if len(full_name) <= 63:
            return full_name

        hash_suffix = sha256(cols_slug.encode("utf-8")).hexdigest()[:8]
        prefix = full_name[:54]
        return f"{prefix}_{hash_suffix}"

    @classmethod # get columns snapshot
    def get_columns_snapshot(cls, 
                             format: Literal['json', 'dict', 'list']
    ) -> str | dict | list:
        """
        Returns a representation of the table schema (field names and types) in different formats.

        Types are also converted to Polar types with the _convert_to_polar_types() function. 
        """
        columns_snapshot = cls._get_columns_snapshot_dict(is_clean=True)

        if format == 'json':
            return msgspec.json.encode(columns_snapshot).decode('utf-8')
        elif format == 'dict':
            return columns_snapshot
        elif format == 'list':
            return [name for name, field in cls.model_fields.items()]
        
    @classmethod # get hash signature
    def get_signature(cls) -> str:
        """
        Generates a signature of the table schema.
        """
        columns_snapshot = cls._get_columns_snapshot_dict(is_clean=False)
        
        payload = msgspec.json.encode(columns_snapshot)

        return sha256(payload).hexdigest()