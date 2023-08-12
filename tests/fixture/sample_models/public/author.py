from __future__ import annotations
from typing import TYPE_CHECKING, Any, ForwardRef, Optional

from geojson_pydantic import Polygon
from pydantic import BaseModel

from cuckoo.models.aggregate import AggregateResponse
from cuckoo.models.common import (
    HasuraTableModel,
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
)

if TYPE_CHECKING:
    from tests.fixture.sample_models.public.author_detail import AuthorDetail
    from tests.fixture.sample_models.public.article import (
        Article,
        ArticleAggregate,
    )
else:
    AuthorDetail = ForwardRef("AuthorDetail")
    Article = ForwardRef("Article")
    ArticleAggregate = ForwardRef("ArticleAggregate")


class AuthorBase(
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
):
    """
    Contains all properties, but no foreign relations or aggregates. Used for `min` and
    `max` aggregation results as well as a base class.
    Note: DO NOT inhert from `HasuraTableModel` -> pydantic error
    """

    name: Optional[str]
    age: Optional[int]
    home_zone: Optional[Polygon]
    jsonb_list: Optional[list[Any]]
    jsonb_dict: Optional[dict[str, Any]]


class AuthorNumerics(BaseModel):
    """
    Contains only the numeric properties of the model as `float`. Used for all aggregate
    results, except for `min` and `max`.
    """

    age: Optional[float]


class Author(HasuraTableModel, AuthorBase):
    """
    The table model with all own properties inherited from the base class and foreign
    relations/aggregations being defined here.
    """

    _table_name = "authors"
    detail: Optional[AuthorDetail]
    articles: Optional[list[Article]]
    articles_aggregate: Optional[ArticleAggregate]


AuthorAggregate = AggregateResponse[AuthorBase, AuthorNumerics, Author]
