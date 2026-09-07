# third party
import patito as pt
# local
from .BASE import BaseIGDBSchema

# 4. Endpoint: /platforms
class PlatformSchema(BaseIGDBSchema):
    _endpoint  = "/platforms"
    _full_load = True
    _if_table_exists = 'replace'
    
    id: int = pt.Field(unique=True)
    name: str
    slug: str | None = None
    abbreviation: str | None = None
    alternative_name: str | None = None
    generation: int | None = None
    platform_family: int | None = None  # ID de la famille (PlayStation, Xbox, Nintendo)

    created_at: int | None = None
    updated_at: int | None = None
    checksum: str | None = None

