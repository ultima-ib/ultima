"""
Core Ultima functionality.

The modules within `ultima.internals` are interdependent. To prevent cyclical imports,
they all import from each other via this __init__ file using
`import ultima.internals as uli`. The imports below are being shared across this module.
"""

from .agg_request import AggRequest
from .dataset import DataSet, FRTBDataSet, DS

__all__ = ["AggRequest", "FRTBDataSet", "DataSet", "DS"]
