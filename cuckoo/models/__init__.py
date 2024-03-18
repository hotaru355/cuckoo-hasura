from .builtins import (
    TBATCH_MODEL,
    TBATCHMODEL_BASE,
    TBATCHNUM_PROPS,
    TMODEL,
    TMODEL_BASE,
    TNUM_PROPS,
    Aggregate,
    AggregateResponse,
    TableModel,
)
from .common import (
    CreatableModel,
    DeletableModel,
    IdentityModel,
    UntypedModel,
    UpdatableModel,
)
from .providers import (
    PydanticV1TableModel,
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
