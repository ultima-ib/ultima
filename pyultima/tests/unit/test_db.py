import unittest

import polars as pl

import ultibi as ul


class TestDb(unittest.TestCase):

    def setUp(self):
        ds = ul.FRTBDataSet.from_config_path("./tests/data/datasource_config.toml")

        uri="mysql://{0}:{1}@{2}:{3}/{4}".format(
                    "dummyUser", "password", "localhost", 3306, "ultima")

        df = ds.frame()
        df = df.drop(["SensWeights", "CurvatureRiskWeight", "SensWeightsCRR2", "CurvatureRiskWeightCRR2", "ScaleFactor", "SeniorityRank"])
        df.write_database("frtb", uri, if_exists="replace")

    def test_mysql(self):
        conn_uri = "mysql://%s:%s@%s:%d/%s?cxprotocol=binary"%("dummyUser", "password", "localhost", 3306, "ultima")

        db = ul.DbInfo("frtb", "MySQL", conn_uri, self.schema())

        source = ul.DataSource.db(db)

        build_params = dict(
            fx_sqrt2_div = "true",
            girr_sqrt2_div = "true",
            csrnonsec_covered_bond_15 = "true",
            DayCountConvention = "2",
            DateFormat = "DateFormat"
        )

        ds = ul.FRTBDataSet.from_source(source, build_params=build_params)
    
    def schema(self):
        return [
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
        ("SensitivityCcy", pl.Utf8),
        ("CoveredBondReducedWeight", pl.Utf8),
        ("Sector", pl.Utf8),
        ("FxCurvDivEligibility", pl.Boolean),
        ("BookId", pl.Utf8),
        ("Product", pl.Utf8),
        ("Notional", pl.Float64),
        ("Desk", pl.Utf8),
        ("Country", pl.Utf8),
        ("LegalEntity", pl.Utf8),
        ("Group", pl.Utf8),
        ("RiskCategory", pl.Utf8),
        ("RiskClass", pl.Utf8),
        ("RiskFactor", pl.Utf8),
        ("RiskFactorType", pl.Utf8),
        ("CreditQuality", pl.Utf8),
        ("MaturityDate", pl.Utf8),
        ("Tranche", pl.Utf8),
        ("CommodityLocation", pl.Utf8),
        ("GirrVegaUnderlyingMaturity", pl.Utf8),
        ("BucketBCBS", pl.Utf8),
        ("BucketCRR2", pl.Utf8),
        ("GrossJTD", pl.Float64),
        ("PnL_Up", pl.Float64),
        ("PnL_Down", pl.Float64),
        ]

if __name__ == "__main__":
    unittest.main()