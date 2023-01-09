import ultima as ul

ds = ul.FRTBDataSet.from_config_path("./data/frtb/datasource_config.toml")
ds.prepare()
ds.measures()
ul.aggregation_ops()

# What do we want to calculate?
request = dict(
    measures=[
        ["DRC_NonSec_GrossJTD", "sum"],
        ["DRC_NonSec_GrossJTD_Scaled", "sum"],
        ["DRC_NonSec_CapitalCharge", "scalar"],
        ["DRC_NonSec_NetLongJTD", "scalar"],
        ["DRC_NonSec_NetShortJTD", "scalar"],
        ["DRC_NonSec_NetLongJTD_Weighted", "scalar"],
        ["DRC_NonSec_NetAbsShortJTD_Weighted", "scalar"],
        ["DRC_NonSec_HBR", "scalar"],
    ],
    groupby=["Desk", "BucketBCBS"],
    hide_zeros=True,
    calc_params={
        "jurisdiction": "BCBS",
        "apply_fx_curv_div": "true",
        "drc_offset": "true",
    },
)

request = ul.AggRequest(request)
# print(request)

result = ul.execute_agg(request, ds)
# print(result)
# print("Type: ", type(result))
