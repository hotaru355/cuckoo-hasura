from typing import TYPE_CHECKING, ForwardRef, Optional
from uuid import UUID

from cuckoo.models.common import (
    HasuraTableModel,
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
)

if TYPE_CHECKING:
    from tests.fixture.sample_models.public import Author, Address, DetailsAddresses
else:
    Author = ForwardRef("Author")
    Address = ForwardRef("Address")
    DetailsAddresses = ForwardRef("DetailsAddresses")


class AuthorDetailBase(
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
):
    author_uuid: Optional[UUID]
    primary_address_uuid: Optional[UUID]
    secondary_address_uuid: Optional[UUID]
    country: Optional[str]


class AuthorDetail(
    HasuraTableModel,
    AuthorDetailBase,
):
    _table_name = "author_details"
    author: Optional[Author]
    primary_address: Optional[Address]
    secondary_address: Optional[Address]
    past_primary_addresses: Optional[list[DetailsAddresses]]
    past_secondary_addresses: Optional[list[DetailsAddresses]]
