import polars as pl
from pyfrtb import AggregationRequestWrapper, FRTBDataSetWrapper, _execute_agg

def execute_agg(req: AggregationRequestWrapper, ds: FRTBDataSetWrapper) -> pl.DataFrame:
    vec_srs = _execute_agg(req, ds)
    return pl.DataFrame(vec_srs)
