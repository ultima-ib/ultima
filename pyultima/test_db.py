# Tmp file to test the Db
# This will be moved to a proper unittest

import ultibi as ul
import polars as pl

conn_uri = "mysql://%s:%s@%s:%d/%s?cxprotocol=binary"%("dummyUser", "password", "localhost", 3306, "ultima")

schema = [
        ("COB", pl.Utf8),
        ("TradeId", pl.Utf8),
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
        ("SensitivitySpot", pl.Float64),
        ("Sensitivity_025Y", pl.Float64),
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
        ("EXOTIC_RRAO", pl.Boolean),
        ("OTHER_RRAO", pl.Boolean),
        ("Notional", pl.Float64),
        ("Desk", pl.Utf8),
        ("Country", pl.Utf8),
        ("LegalEntity", pl.Utf8),
        ("Group", pl.Utf8)
]

db = ul.DbInfo("frtb", "MySQL", conn_uri, schema)

source = ul.DataSource.db(db)

source.inner

build_params = dict(
    fx_sqrt2_div = "true",
    girr_sqrt2_div = "true",
    csrnonsec_covered_bond_15 = "true",
    DayCountConvention = "2",
    DateFormat = "DateFormat"
)

ds = ul.FRTBDataSet.from_source(source, build_params=build_params)

ds.ui()

# scan = pl.scan_csv(
#             "./frtb_engine/data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64}
#         )

# dsource = ul.DataSource.scan(scan)

# dsource.inner 
# ds = ul.DataSet.from_source(dsource)

