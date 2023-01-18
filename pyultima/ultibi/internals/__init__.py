"""
Core Ultima functionality.

The modules within `ultima.internals` are interdependent. To prevent cyclical imports,
they all import from each other via this __init__ file using
`import ultibi.internals as uli`. The imports below are being shared across this module.
"""

from ..rust_module.ultima_pyengine import NoDataError, OtherError
from .agg_request import AggRequest, aggregation_ops
from .dataset import DS, DataSet, FRTBDataSet
from .execute import execute_agg

__all__ = [
    "execute_agg",
    "AggRequest",
    "aggregation_ops",
    "FRTBDataSet",
    "DataSet",
    "DS",
    "NoDataError",
    "OtherError",
]
