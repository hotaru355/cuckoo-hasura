from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

from cuckoo.models.common import TMODEL


TNUM_PROPS = TypeVar("TNUM_PROPS")
TMODEL_BASE = TypeVar("TMODEL_BASE")
TBATCHNUM_PROPS = TypeVar("TBATCHNUM_PROPS")
TBATCHMODEL_BASE = TypeVar("TBATCHMODEL_BASE")


class Aggregate(GenericModel, Generic[TMODEL_BASE, TNUM_PROPS]):
    count: Optional[int]
    avg: Optional[TNUM_PROPS]
    max: Optional[TMODEL_BASE]
    min: Optional[TMODEL_BASE]
    stddev: Optional[TNUM_PROPS]
    stddev_pop: Optional[TNUM_PROPS]
    stddev_samp: Optional[TNUM_PROPS]
    sum: Optional[TNUM_PROPS]
    var_pop: Optional[TNUM_PROPS]
    var_samp: Optional[TNUM_PROPS]
    variance: Optional[TNUM_PROPS]


class AggregateResponse(GenericModel, Generic[TMODEL_BASE, TNUM_PROPS, TMODEL]):
    aggregate: Optional[Aggregate[TMODEL_BASE, TNUM_PROPS]]
    nodes: Optional[list[TMODEL]]
