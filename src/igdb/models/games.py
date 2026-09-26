# third party
from sqlalchemy import false
import patito as pt
# local
from .BASE import BaseIGDBSchema

# 1. Endpoint: /games
class GameSchema(BaseIGDBSchema):
    _endpoint = "/games"
    _conserve_history = True
    _index_at = (
        # in the Dashboard every analytical query filters on _is_current first.
        # Compound indexes let Postgres do an index-only scan instead of a full sequential scan on ~200k+ rows
        #
        # 1. (_is_current, rating)        → top-rated games, KPI rated_games
        # 2. (_is_current, first_release_date) → timeline chart, game explorer sort by recent
        # 3. (_is_current, hypes)         → game explorer sort by hypes
        # 4. (name,)                      → ILIKE search (prefix scan helps even w/ wildcards)
        ("_is_current", "rating"),
        ("_is_current", "first_release_date"),
        ("_is_current", "hypes"),
        "name",
    )

    id: int = pt.Field(unique=True)
    name: str
    slug: str | None = None
    summary: str | None = None
    storyline: str | None = None

    # Timestamps
    created_at: int | None = None
    updated_at: int | None = None
    first_release_date: int | None = None

    # Métriques d'évaluation et Popularité
    rating: float | None = None
    rating_count: int | None = None
    aggregated_rating: float | None = None
    aggregated_rating_count: int | None = None
    total_rating: float | None = None
    total_rating_count: int | None = None
    hypes: int | None = None

    # Classification
    game_type: int | None = None
    game_status: int | None = None

    # Relations (IGDB renvoie des tableaux d'IDs)
    genres: list[int] = pt.Field(default_factory=list)
    platforms: list[int] = pt.Field(default_factory=list)
    involved_companies: list[int] = pt.Field(default_factory=list)
    release_dates: list[int] = pt.Field(default_factory=list)
    game_modes: list[int] = pt.Field(default_factory=list)
    themes: list[int] = pt.Field(default_factory=list)
    collections: list[int] = pt.Field(default_factory=list)

    # Hiérarchie et metadata
    parent_game: int | None = None
    version_parent: int | None = None
    version_title: str | None = None
    checksum: str | None = None

