from typing import Optional

from cuckoo.models import (
    CreatableModel,
    DeletableModel,
    HasuraTableModel,
    IdentityModel,
    UpdatableModel,
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
