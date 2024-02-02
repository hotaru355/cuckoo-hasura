from typing import TYPE_CHECKING, ForwardRef, Optional
from uuid import UUID

from cuckoo.models import (
    CreatableModel,
    HasuraTableModel,
    IdentityModel,
)

if TYPE_CHECKING:
    from tests.fixture.sample_models.public import Address, AuthorDetail
else:
    AuthorDetail = ForwardRef("AuthorDetail")
    Address = ForwardRef("Address")


class DetailsAddresses(
    IdentityModel,
    CreatableModel,
    HasuraTableModel,
):
    _table_name = "details_addresses"
    author_detail_uuid: Optional[UUID]
    address_uuid: Optional[UUID]
    is_primary: Optional[bool]

    author_detail: Optional[AuthorDetail]
    address: Optional[Address]
