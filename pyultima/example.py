import polars as pl

from ultima.internals.agg_request import AggRequest
from ultima.internals.dataset import DataSet, FRTBDataSet
from ultima.internals.execute import execute_agg

dataset = FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

# Assigning weights. Keeping it separate since it can be done once only
dataset.prepare()
# If you want to play around - check which measures are supported
print(dataset.measures)

# What do we want to calculate?
_request = r"""{"measures": [
    ["DRC_NonSec_GrossJTD", "sum"],["DRC_NonSec_GrossJTD_Scaled","sum"],
    ["DRC_NonSec_CapitalCharge", "scalar"],["DRC_NonSec_NetLongJTD", "scalar"],
    ["DRC_NonSec_NetShortJTD", "scalar"],["DRC_NonSec_NetLongJTD_Weighted", "scalar"],
    ["DRC_NonSec_NetAbsShortJTD_Weighted", "scalar"],["DRC_NonSec_HBR", "scalar"]], 
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
result = execute_agg(request, dataset)

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
result = execute_agg(rr, ds)

print(result)
