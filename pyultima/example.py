import polars as pl

from pyultima.ultibi.internals.requests import AggRequest  # type: ignore[import]
from ultibi.internals.dataset import DataSet, FRTBDataSet

dataset = FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

# Assigning weights. Keeping it separate since it can be done once only
dataset.prepare()
# If you want to play around - check which measures are supported
print(dataset.measures)

# What do we want to calculate?
_request = r"""{"measures": [
    ["DRC nonSec GrossJTD", "sum"],["DRC nonSec GrossJTD Scaled","sum"],
    ["DRC nonSec CapitalCharge", "scalar"],["DRC nonSec NetLongJTD", "scalar"],
    ["DRC nonSec NetShortJTD", "scalar"],["DRC nonSec NetLongJTD Weighted", "scalar"],
    ["DRC nonSec NetAbsShortJTD Weighted", "scalar"],["DRC nonSec HBR", "scalar"]], 
    "groupby": ["Desk", "BucketBCBS"],
     "type": "AggregationRequest", 
    "hide_zeros": false,
     "calc_params": { "jurisdiction": "BCBS",
     "apply_fx_curv_div": "true", "drc_offset": "false" }}
      """
request = AggRequest(_request)
print(request)

# Execute request
# All runs in parallel
result = dataset.compute(request)

print("Result: ", result)
print("Type: ", type(result))

# General, Non FRTB example
data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
df = pl.DataFrame(data)
ds = DataSet.from_frame(df)
print(ds.measures)

r = dict(
    measures=[("a", "mean"), ("b", "sum")],
    groupby=["c"],
    totals=False,
    calc_params={"jurisdiction": "BCBS"},
)
rr = AggRequest(r)
result = ds.compute(rr)

print(result)
