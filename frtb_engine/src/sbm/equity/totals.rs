use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn eq_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("EQ DeltaCharge Low") + col("EQ VegaCharge Low") + col("EQ CurvatureCharge Low"))
}
pub(crate) fn eq_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("EQ DeltaCharge Medium")
        + col("EQ VegaCharge Medium")
        + col("EQ CurvatureCharge Medium"))
}
pub(crate) fn eq_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("EQ DeltaCharge High") + col("EQ VegaCharge High") + col("EQ CurvatureCharge High"))
}

pub(crate) fn eq_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "EQ TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(eq_total_low),
            depends_upon: vec![
                ("EQ DeltaCharge Low".to_string(), "scalar".to_string()),
                ("EQ VegaCharge Low".to_string(), "scalar".to_string()),
                ("EQ CurvatureCharge Low".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "EQ TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(eq_total_medium),
            depends_upon: vec![
                ("EQ DeltaCharge Medium".to_string(), "scalar".to_string()),
                ("EQ VegaCharge Medium".to_string(), "scalar".to_string()),
                (
                    "EQ CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "EQ TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(eq_total_high),
            depends_upon: vec![
                ("EQ DeltaCharge High".to_string(), "scalar".to_string()),
                ("EQ VegaCharge High".to_string(), "scalar".to_string()),
                ("EQ CurvatureCharge High".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
    ]
}
