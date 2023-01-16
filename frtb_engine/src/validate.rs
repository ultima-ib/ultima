use base_engine::polars::prelude::{PolarsResult, LazyFrame, PolarsError};

pub(crate)fn validate_frame(lf: &LazyFrame) -> PolarsResult<()>{
    // These columns are needed for weights assignments 
    let arc_schema = lf.schema()?;
    
    let mut must_have = vec![
        "RiskClass",
        "RiskCategory",
        "RiskFactorType",
        "BucketBCBS",
        "CreditQuality",
        "CoveredBondReducedWeight",];
    
    //let csrnonsec_covered_bond_15 = self
    //    .build_params
    //    .get("csrnonsec_covered_bond_15")
    //    .and_then(|s| s.parse::<bool>().ok())
    //    .unwrap_or_else(|| false);

    //if csrnonsec_covered_bond_15
    
    if cfg!(feature = "CRR2") {
        must_have.push("BucketCRR2")
     }
    
    for must_have_col in must_have {
        if !arc_schema.iter_names().any(|col|col==must_have_col)
            {
                return Err(
                    PolarsError::NoData(format!("{must_have_col} is missing. It is a required column. Check your data").into())
                ) 
            }
    }

    Ok(())

}