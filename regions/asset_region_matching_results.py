from typing import TYPE_CHECKING, Optional, ForwardRef
from uuid import UUID


from cuckoo.models import HasuraTableModel

if TYPE_CHECKING:
    from tables.regions import AssetRegion
else:
    AssetRegion = ForwardRef("AssetRegion")


class AssetRegionMatchingResults(HasuraTableModel):
    _table_name = "asset_region_matching_results"

    source_asset_region_uuid: Optional[UUID]
    target_asset_region_uuid: Optional[UUID]

    source_asset_region: Optional[AssetRegion]
    target_asset_region: Optional[AssetRegion]
