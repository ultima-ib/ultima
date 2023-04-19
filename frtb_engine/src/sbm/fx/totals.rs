use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn fx_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX DeltaCharge Low") + col("FX VegaCharge Low") + col("FX CurvatureCharge Low"))
}
pub(crate) fn fx_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX DeltaCharge Medium")
        + col("FX VegaCharge Medium")
        + col("FX CurvatureCharge Medium"))
}
pub(crate) fn fx_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX DeltaCharge High") + col("FX VegaCharge High") + col("FX CurvatureCharge High"))
}

pub(crate) fn fx_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "FX TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(fx_total_low),
            depends_upon: vec![
                ("FX DeltaCharge Low".to_string(), "scalar".to_string()),
                ("FX VegaCharge Low".to_string(), "scalar".to_string()),
                ("FX CurvatureCharge Low".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "FX TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(fx_total_medium),
            depends_upon: vec![
                ("FX DeltaCharge Medium".to_string(), "scalar".to_string()),
                ("FX VegaCharge Medium".to_string(), "scalar".to_string()),
                (
                    "FX CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "FX TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(fx_total_high),
            depends_upon: vec![
                ("FX DeltaCharge High".to_string(), "scalar".to_string()),
                ("FX VegaCharge High".to_string(), "scalar".to_string()),
                ("FX CurvatureCharge High".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
    ]
}
