import os
import sys
import unittest

import polars as pl
import pytest

# import pytest
from polars.type_aliases import PolarsDataType

import ultibi as ul

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"
LINUX = (sys.platform == "linux") or (sys.platform == "linux2")


class TestDb(unittest.TestCase):
    """REQUIRES A RUNNING SQL SERVICE. If running locally create MySql Service and table"""

    def host(self) -> str:
        if IN_GITHUB_ACTIONS:
            print("GITHUB ACTIONS")
            return "127.0.0.1"
        else:
            return "localhost"

    def setUp(self) -> None:
        ds = ul.FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

        uri = "mysql://{0}:{1}@{2}:{3}/{4}".format(
            "root", "mysql", self.host(), 3306, "ultima"
        )

        df = ds.frame()
        df = df.drop(
            [
                "SensWeights",
                "CurvatureRiskWeight",
                "SensWeightsCRR2",
                "CurvatureRiskWeightCRR2",
                "ScaleFactor",
                "SeniorityRank",
            ]
        )
        df.write_database("frtb", uri, if_table_exists="replace")

    @pytest.mark.skipif(
        IN_GITHUB_ACTIONS and (not LINUX),
        reason="Test doesn't work in Github Actions Windows",
    )
    def test_mysql(self) -> None:
        conn_uri = "mysql://%s:%s@%s:%d/%s?cxprotocol=binary" % (
            "root",
            "mysql",
            self.host(),
            3306,
            "ultima",
        )

        db = ul.DbInfo("frtb", "MySQL", conn_uri, self.schema())

        source = ul.DataSource.db(db)

        build_params = dict(
            fx_sqrt2_div="true",
            girr_sqrt2_div="true",
            csrnonsec_covered_bond_15="true",
            DayCountConvention="2",
            DateFormat="DateFormat",
        )

        ds = ul.FRTBDataSet.from_source(source, build_params=build_params)

        request = dict(
            measures=[
                ["FX DeltaCharge Low", "scalar"],
                ["FX DeltaCharge Medium", "scalar"],
                ["FX DeltaCharge High", "scalar"],
            ],
            groupby=["Desk"],
            filters=[[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
            hide_zeros=True,
            calc_params={
                "jurisdiction": "BCBS",
                "apply_fx_curv_div": "true",
                "drc_offset": "true",
            },
        )

        res = ds.compute(ul.ComputeRequest(request))

        assert res.row(0)[1:] == (
            11.652789365641173,
            11.80386589215584,
            11.95303308788192,
        )

    def schema(self) -> list[tuple[str, PolarsDataType]]:
        schema: list[tuple[str, PolarsDataType]] = [
            ("COB", pl.Utf8),
            ("TradeId", pl.Utf8),
            ("SensitivitySpot", pl.Float64),
            ("Sensitivity_025Y", pl.Float64),
            ("EXOTIC_RRAO", pl.Boolean),
            ("OTHER_RRAO", pl.Boolean),
            ("Sensitivity_05Y", pl.Float64),
            ("Sensitivity_1Y", pl.Float64),
            ("Sensitivity_2Y", pl.Float64),
            ("Sensitivity_3Y", pl.Float64),
            ("Sensitivity_5Y", pl.Float64),
            ("Sensitivity_10Y", pl.Float64),
            ("Sensitivity_15Y", pl.Float64),
            ("Sensitivity_20Y", pl.Float64),
            ("Sensitivity_30Y", pl.Float64),
            ("SensitivityCcy", pl.String),
            ("CoveredBondReducedWeight", pl.String),
            ("Sector", pl.String),
            ("FxCurvDivEligibility", pl.Boolean),
            ("BookId", pl.String),
            ("Product", pl.String),
            ("Notional", pl.Float64),
            ("Desk", pl.String),
            ("Country", pl.String),
            ("LegalEntity", pl.String),
            ("Group", pl.String),
            ("RiskCategory", pl.String),
            ("RiskClass", pl.String),
            ("RiskFactor", pl.String),
            ("RiskFactorType", pl.String),
            ("CreditQuality", pl.String),
            ("MaturityDate", pl.String),
            ("Tranche", pl.String),
            ("CommodityLocation", pl.String),
            ("GirrVegaUnderlyingMaturity", pl.String),
            ("BucketBCBS", pl.String),
            ("BucketCRR2", pl.String),
            ("GrossJTD", pl.Float64),
            ("PnL_Up", pl.Float64),
            ("PnL_Down", pl.Float64),
        ]
        return schema


if __name__ == "__main__":
    unittest.main()
