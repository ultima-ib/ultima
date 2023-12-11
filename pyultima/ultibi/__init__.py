"""
DataFrame UI, Pivot, Cube technology.
FRTB use case comes ready out of the box.
"""

import polars  # reexport

from .internals import (
    AggRequest,
    BaseMeasure,
    CalcParam,
    ComputeRequest,
    CustomCalculator,
    DataSet,
    DataSource,
    DbInfo,
    DependantMeasure,
    EqFilter,
    FRTBDataSet,
    InFilter,
    NeqFilter,
    NoDataError,
    NotInFilter,
    OtherError,
    RustCalculator,
    StandardCalculator,
    UltimaError,
    aggregation_ops,
)

# import pyarrow - not needed


__all__ = [
    "AggRequest",
    "ComputeRequest",
    "FRTBDataSet",
    "DataSet",
    "DataSource",
    "aggregation_ops",
    "OtherError",
    "NoDataError",
    "polars",
    "pyarrow",
    "Filter",
    "FilterType",
    "EqFilter",
    "NeqFilter",
    "InFilter",
    "NotInFilter",
    "Measure",
    "BaseMeasure",
    "DependantMeasure",
    "Calculator",
    "CustomCalculator",
    "StandardCalculator",
    "CalcParam",
    "UltimaError",
    "DbInfo",
    "RustCalculator",
]
