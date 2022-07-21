use base_engine::prelude::*;


use polars::prelude::*;

/// Sum of all delta sensis, from spot to 30Y tenor
/// In practice should be used only with filter on RiskClass
/// as combining FX and IR sensis is meaningless
pub fn total_delta_sens() -> Expr {
    // When adding Exprs NULLs have to be filled
    // Otherwise returns NULL
    col("SensitivitySpot").fill_null(0.)
    +col("Sensitivity_025Y").fill_null(0.)
    +col("Sensitivity_05Y").fill_null(0.)
    +col("Sensitivity_1Y").fill_null(0.)
    +col("Sensitivity_2Y").fill_null(0.) 
    +col("Sensitivity_3Y").fill_null(0.)
    +col("Sensitivity_5Y").fill_null(0.)
    +col("Sensitivity_10Y").fill_null(0.)
    +col("Sensitivity_15Y").fill_null(0.)
    +col("Sensitivity_20Y").fill_null(0.)
    +col("Sensitivity_30Y").fill_null(0.)
}

/// WhenThen shouldn't be used inside groupby
/// this works so far
/// but this function is NOT to be relied on in calculation
pub fn rc_delta_sens(rc: &str) -> Expr {
    when(
        col("RiskClass").eq(lit(rc)).and(
            col("RiskCategory").eq(lit("Delta")))
    )
    .then(total_delta_sens()) 
    .otherwise(lit::<f64>(0.0))
}

/// Helper function to derive weighted delta,
/// per tenor, per risk class, per risk Category
/// TODO allow SensWeights OR SensWeights depending on Reporing.
pub fn rc_tenor_weighted_sens(rcat: &'static str, rc: &'static str, delta_tenor: &str, weights_col: &str, weight_idx: i64) -> Expr {

    apply_multiple(  move |columns| {
         
        //RiskClass
        let mask = columns[0]
            //.utf8()?
            .equal(rc)?;
        //RiskCategory
        let mask1 = columns[3]
            .utf8()?
            .equal(rcat);
        
        let delta = columns[1]
            .f64()?
            .set(&!(mask&mask1), None)?;
        
        let x = delta.multiply(&columns[2])?;
        Ok(x)
    }, 
        &[col("RiskClass"), col(delta_tenor), col(weights_col).arr().get(weight_idx), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
}

///makes sence at RiskClass-Bucket view
pub fn sens_weights(_: &OCP) -> Expr {
    col("SensWeights")
}