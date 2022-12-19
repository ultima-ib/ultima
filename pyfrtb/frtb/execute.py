import polars as pl
from .rust_module.frtb_pyengine import AggregationRequestWrapper, FRTBDataSetWrapper, _execute_agg

def execute_agg(req: AggregationRequestWrapper, ds: FRTBDataSetWrapper) -> pl.DataFrame:
    """Executes Request on your DataSet

    Args:
        req (AggregationRequestWrapper): Aggregation Request - defines what you want to calculate.
        ds (FRTBDataSetWrapper): Your DataSet

    Returns:
        pl.DataFrame: Result of type Polars DataFrame
    """
    vec_srs = _execute_agg(req, ds)
    return pl.DataFrame(vec_srs)
