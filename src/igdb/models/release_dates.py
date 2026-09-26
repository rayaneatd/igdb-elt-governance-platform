# third party
import patito as pt
# local
from .BASE import BaseIGDBSchema

# 2. Endpoint: /release_dates
class ReleaseDateSchema(BaseIGDBSchema):
    _endpoint = "/release_dates"
    _index_at = (
        # game → lookup all release dates for a given game ID (FK join)
        # y    → filter by release year in future timeline queries
        "game",
        "y",
    )

    id: int = pt.Field(unique=True)
    game: int | None = None
    platform: int | None = None
    date: int | None = None           # Timestamp Unix
    human: str | None = None          # Ex: "2026-Q3" ou "Dec 31, 2026"
    y: int | None = None              # Année de sortie (e.g. 2026)
    m: int | None = None              # Mois (1-12)
    region: int | None = None         # Region Enum (Europe, US, JP...)
    status: int | None = None         # Release Date Status Enum

    created_at: int | None = None
    updated_at: int | None = None
    checksum: str | None = None

