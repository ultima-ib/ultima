import polars as pl

from ultima.execute import execute_agg
from ultima.internals.agg_request import AggRequest
from ultima.internals.dataset import DataSet, FRTBDataSet

# pl.DataFrame

dataset = FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

# print("dataset ", dataset)

# Tried to use formatted json via raw literals but something went wrong
_request = r"""{"measures": [
    ["DRC_NonSec_GrossJTD", "sum"],["DRC_NonSec_GrossJTD_Scaled","sum"],
    ["DRC_NonSec_CapitalCharge", "scalar"],["DRC_NonSec_NetLongJTD", "scalar"],
    ["DRC_NonSec_NetShortJTD", "scalar"],["DRC_NonSec_NetLongJTD_Weighted", "scalar"],
    ["DRC_NonSec_NetAbsShortJTD_Weighted", "scalar"],["DRC_NonSec_HBR", "scalar"]], 
    "groupby": ["Desk", "BucketBCBS"], "type": "AggregationRequest", 
    "hide_zeros": false, "calc_params": { "jurisdiction": "BCBS",
     "apply_fx_curv_div": "true", "drc_offset": "false" }}
      """
# request3 = ultima_pyengine.AggregationRequestWrapper.from_str(request)
request = AggRequest(_request)

print(request)

dataset.prepare()
print(dataset.measures())
result = execute_agg(request, dataset)

print("Result: ", result)
print("Type: ", type(result))

data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "a", "b"]}
df = pl.DataFrame(data)
ds = DataSet.from_frame(df)
print(ds.measures())

r = dict(
    measures=[("a", "mean"), ("b", "sum")],
    groupby=["c"],
    totals=False,
    calc_params={"jurisdiction": "BCBS"},
)
rr = AggRequest(r)
result = execute_agg(rr, ds)

print(result)
