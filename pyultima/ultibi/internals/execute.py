from __future__ import annotations

import polars as pl

from ultibi.internals import DS, AggRequest

from ..rust_module.ultima_pyengine import exec_agg


def execute_agg(req: AggRequest, ds: DS, streaming: bool = False) -> pl.DataFrame:
    """Executes Aggregation Request(which runs in groupby().apply() context)
     on your DataSet

    Args:
        req (AggregationRequestWrapper): Aggregation Request - defines what you want to
         calculate.
        ds (FRTBDataSetWrapper): Your DataSet
        streaming (bool): enables streaming feature of polars. Can be used to procees
         larger than RAM datasets

    Returns:
        pl.DataFrame: Result of type Polars DataFrame
    """
    vec_srs = exec_agg(req._ar, ds._ds, streaming)
    return pl.DataFrame(vec_srs)
