from datetime import date
from enum import Enum
from typing import Optional

from cuckoo.models import (
    HasuraTableModel,
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
)


class AssetRegionTypes(
    IdentityModel, CreatableModel, UpdatableModel, DeletableModel, HasuraTableModel
):
    _table_name = "asset_region_types"

    name: Optional[str]
    description: Optional[str]
    is_active_start_on: Optional[date]
    is_active_end_on: Optional[date]


class AssetRegionTypeNames(str, Enum):
    ROW = "row"
    SET = "set"
    STACK = "stack"
    MODULE = "module"
    COMBINER = "combiner"
    INVERTER = "inverter"
