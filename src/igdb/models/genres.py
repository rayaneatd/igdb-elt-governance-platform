# third party
import patito as pt
# local
from .BASE import BaseIGDBSchema


# 3. Endpoint: /genres
class GenreSchema(BaseIGDBSchema):
    _endpoint  = "/genres"
    _full_load = True

    id: int = pt.Field(unique=True)
    name: str
    slug: str | None = None

    created_at: int | None = None
    updated_at: int | None = None
    checksum: str | None = None

