from datetime import date
from typing import TYPE_CHECKING, Any, ForwardRef, Optional
from uuid import UUID

from geojson_pydantic import Polygon

from cuckoo.models import (
    CreatableModel,
    DeletableModel,
    HasuraTableModel,
    IdentityModel,
    UpdatableModel,
)


class AssetRegion(
    IdentityModel, CreatableModel, UpdatableModel, DeletableModel, HasuraTableModel
):
    _table_name = "asset_regions"

    asset_region_global_uuid: Optional[UUID]
    site_uuid: Optional[UUID]
    asset_region_type_uuid: Optional[UUID]
    draft_version_uuid: Optional[UUID]
    geometry: Optional[Polygon]
    nom_stack: Optional[str]
    nom_row: Optional[str]
    nom_set: Optional[str]
    nom_module: Optional[str]
    nom_properties: Optional[dict[str, Any]]
    is_active_start_on: Optional[date]
    is_active_end_on: Optional[date]
