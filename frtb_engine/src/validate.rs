use ultibi::{
    errors::{UltiResult, UltimaErr},
    polars::prelude::{LazyFrame, PolarsError},
};

pub(crate) fn validate_frtb_frame(lf: &LazyFrame, covered_bond: bool, v: u8) -> UltiResult<()> {
    let arc_schema = lf.schema()?;

    // Buckets and weights assignments
    let mut must_have = vec![
        "RiskClass",
        "RiskCategory",
        "RiskFactor",
        "RiskFactorType",
        "BucketBCBS",
        "CreditQuality",
        "PnL_Up",
        "PnL_Down",
        "COB",
        "MaturityDate",
    ];

    if covered_bond {
        must_have.push("CoveredBondReducedWeight")
    }

    if cfg!(feature = "CRR2") {
        must_have.push("BucketCRR2")
    }

    if matches!(v, 0) {
        // RRAO
        must_have.push("TradeId");
        must_have.push("EXOTIC_RRAO");
        must_have.push("OTHER_RRAO");

        // SBM + DRC
        must_have.push("GrossJTD");
        must_have.push("Tranche");
        must_have.push("CommodityLocation");
        must_have.push("GirrVegaUnderlyingMaturity");

        must_have.push("SensitivitySpot");
        must_have.push("Sensitivity_025Y");
        must_have.push("Sensitivity_05Y");
        must_have.push("Sensitivity_1Y");
        must_have.push("Sensitivity_2Y");
        must_have.push("Sensitivity_3Y");
        must_have.push("Sensitivity_5Y");
        must_have.push("Sensitivity_10Y");
        must_have.push("Sensitivity_15Y");
        must_have.push("Sensitivity_20Y");
        must_have.push("Sensitivity_30Y");
    }

    for must_have_col in must_have {
        if !arc_schema.iter_names().any(|col| col == must_have_col) {
            return Err(UltimaErr::from(PolarsError::NoData(
                format!("{must_have_col} is missing. It is a required column. Check your data")
                    .into(),
            )));
        }
    }

    Ok(())
}
