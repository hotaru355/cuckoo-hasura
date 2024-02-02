from .common import (
    TBATCHMODEL_BASE,
    TBATCHNUM_PROPS,
    TMODEL_BASE,
    TNUM_PROPS,
    CreatableModel,
    DeletableModel,
    IdentityModel,
    UntypedModel,
    UpdatableModel,
)
from .table import (
    TBATCH_MODEL,
    TMODEL,
    Aggregate,
    AggregateResponse,
    PydanticV1TableModel,
    TableModel,
)

HasuraTableModel = PydanticV1TableModel
"""Table model, defaults to pydantic V1. To use any other lib, use for example:
`HasuraTableModel = PydanticV2TableModel`"""

__all__ = [
    "UntypedModel",
    "TableModel",
    "TBATCH_MODEL",
    "TBATCHMODEL_BASE",
    "TBATCHNUM_PROPS",
    "TMODEL_BASE",
    "TNUM_PROPS",
    "TMODEL",
    "Aggregate",
    "AggregateResponse",
    "CreatableModel",
    "DeletableModel",
    "HasuraTableModel",
    "IdentityModel",
    "UpdatableModel",
]
