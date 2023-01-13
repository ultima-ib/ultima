import ultibi as ul
import polars as pl

pl.Config.set_tbl_rows(100)
ds = ul.FRTBDataSet.from_config_path("./data/frtb/datasource_config.toml")
ds.prepare()

request = dict(
    filters=[],
    groupby=["RiskClass", "Desk"],
    overrides=[
        {
            "field": "SensWeights",
            "value": "[0.005]",
            "filters": [
                [{"op": "Eq", "field": "RiskClass", "value": "DRC_nonSec"}],
                [{"op": "Eq", "field": "CreditQuality", "value": "AA"}],
            ],
        }
    ],
    measures=[["DRC nonSec CapitalCharge", "scalar"]],
    hide_zeros=True,
    calc_params={
        "jurisdiction": "BCBS",
        "drc_offset": "false",
    },
)
result = ds.execute(request)

request = dict(
    filters=[],
    groupby=["RiskClass", "Desk"],
    measures=[["DRC nonSec CapitalCharge", "scalar"]],
    hide_zeros=True,
    calc_params={
        "jurisdiction": "CRR2",
        "drc_offset": "false",
    },
)
result = ds.execute(request)
