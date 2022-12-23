import polars as pl
from ultima.internals import AggRequest
from ultima.internals import FRTBDataSet

from .rust_module.ultima_pyengine import exec_agg


def execute_agg(
    req: AggRequest, ds: FRTBDataSet, streaming: bool = False
) -> pl.DataFrame:
    """Executes Request on your DataSet

    Args:
        req (AggregationRequestWrapper): Aggregation Request - defines what you want to
         calculate.
        ds (FRTBDataSetWrapper): Your DataSet
        streaming (bool): enables streaming feature of polars. Needed to procees larger
         than RAM datasets

    Returns:
        pl.DataFrame: Result of type Polars DataFrame
    """
    vec_srs = exec_agg(req._ar, ds._ds, streaming)
    return pl.DataFrame(vec_srs)
