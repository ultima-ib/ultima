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
    BaseMeasure, 
    DependantMeasure,
    EqFilter, 
    NeqFilter,
    InFilter, 
    NotInFilter
)

import polars # reexport
import pyarrow

__all__ = [
    "AggRequest",
    "ComputeRequest",
    "FRTBDataSet",
    "DataSet",
    "aggregation_ops",
    "OtherError",
    "NoDataError",
    "polars",
    "pyarrow",
    "BaseMeasure", 
    "DependantMeasure",
    "Filter", 
    "FilterType",
    "EqFilter", 
    "NeqFilter",
    "InFilter", 
    "NotInFilter"
]
