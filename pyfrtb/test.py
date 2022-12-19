import frtb.rust_module.frtb_pyengine as frtb_pyengine
from frtb.internals.agg_request import AggRequest
from  frtb.execute import execute_agg

# pl.DataFrame

dataset = frtb_pyengine.FRTBDataSetWrapper.from_config_path(
    "./tests/datasource_config.toml")

# print("dataset ", dataset)

# Tried to use formatted json via raw literals but something went wrong
request = r'{"measures": [["DRC_NonSec_GrossJTD", "sum"],["DRC_NonSec_GrossJTD_Scaled", "sum"],["DRC_NonSec_CapitalCharge", "scalar"],["DRC_NonSec_NetLongJTD", "scalar"],["DRC_NonSec_NetShortJTD", "scalar"],["DRC_NonSec_NetLongJTD_Weighted", "scalar"],["DRC_NonSec_NetAbsShortJTD_Weighted", "scalar"],["DRC_NonSec_HBR", "scalar"]], "groupby": ["Desk", "BucketBCBS"], "type": "AggregationRequest", "hide_zeros": false, "calc_params": { "jurisdiction": "BCBS", "apply_fx_curv_div": "true", "drc_offset": "false" }}'
#request3 = frtb_pyengine.AggregationRequestWrapper.from_str(request)
request = AggRequest(request)

print(request)

dataset.prepare()
print("prepared_dataset ", dataset)
result = execute_agg(request, dataset)

#result = pyultima._execute_agg(request, dataset)
#res = result.new_agg_result()
print("Result: ", result)
print("Type: ", type(result))
