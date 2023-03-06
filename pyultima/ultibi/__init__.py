"""
FRTB usecase specific library which levrages on ultima's base engine
"""

from .internals import (
    AggRequest,
    ComputeRequest,
    DataSet,
    FRTBDataSet,
    NoDataError,
    OtherError,
    aggregation_ops,
)
from .internals.execute import execute_agg

__all__ = [
    "execute_agg",
    "AggRequest",
    "ComputeRequest",
    "FRTBDataSet",
    "DataSet",
    "aggregation_ops",
    "OtherError",
    "NoDataError",
]
