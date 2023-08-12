from typing import Optional

from cuckoo.models.common import (
    HasuraTableModel,
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
)


class Address(
    CreatableModel,
    DeletableModel,
    HasuraTableModel,
    IdentityModel,
    UpdatableModel,
):
    _table_name = "addresses"
    street: Optional[str]
    postal_code: Optional[str]
    walk_score: Optional[float]
