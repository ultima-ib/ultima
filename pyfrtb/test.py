import ultima
from  ultima.execute import execute_agg

# pl.DataFrame

dataset = ultima.init_frtb_data_set(
    "./tests/datasource_config.toml")

# print("dataset ", dataset)

# Tried to use formatted json via raw literals but something went wrong
request = r'{"measures": [["DRC_NonSec_GrossJTD", "sum"],["DRC_NonSec_GrossJTD_Scaled", "sum"],["DRC_NonSec_CapitalCharge", "scalar"],["DRC_NonSec_NetLongJTD", "scalar"],["DRC_NonSec_NetShortJTD", "scalar"],["DRC_NonSec_NetLongJTD_Weighted", "scalar"],["DRC_NonSec_NetAbsShortJTD_Weighted", "scalar"],["DRC_NonSec_HBR", "scalar"]], "groupby": ["Desk", "BucketBCBS"], "type": "AggregationRequest", "hide_zeros": false, "calc_params": { "jurisdiction": "BCBS", "apply_fx_curv_div": "true", "drc_offset": "false" }}'
request2 = ultima.req_from_str(request)
request3 = ultima.AggregationRequestWrapper.from_str(request)
print(request2)
print(request3)
dataset.prepare()
print("prepared_dataset ", dataset)
result = execute_agg(request, dataset)

#result = pyultima._execute_agg(request, dataset)
#res = result.new_agg_result()
print("new_agg_result ", result)
print("Type: ", type(result))
print("Methods: ", dir(result))
