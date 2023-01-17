use base_engine::polars::prelude::{LazyFrame, PolarsError, PolarsResult};

/// Validate should only check for accurate values
pub(crate) fn validate_frame(lf: &LazyFrame, covered_bond: bool) -> PolarsResult<()> {
    let arc_schema = lf.schema()?;

    // These columns are needed for weights assignments
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
        "BucketCRR2",
    ];

    if covered_bond {
        must_have.push("CoveredBondReducedWeight")
    }

    if cfg!(feature = "CRR2") {
        must_have.push("BucketCRR2")
    }

    for must_have_col in must_have {
        if !arc_schema.iter_names().any(|col| col == must_have_col) {
            return Err(PolarsError::NoData(
                format!("{must_have_col} is missing. It is a required column. Check your data")
                    .into(),
            ));
        }
    }

    Ok(())
}
