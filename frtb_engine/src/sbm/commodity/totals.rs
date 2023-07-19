use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn com_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("Commodity DeltaCharge Low")
        + col("Commodity VegaCharge Low")
        + col("Commodity CurvatureCharge Low"))
}
pub(crate) fn com_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("Commodity DeltaCharge Medium")
        + col("Commodity VegaCharge Medium")
        + col("Commodity CurvatureCharge Medium"))
}
pub(crate) fn com_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("Commodity DeltaCharge High")
        + col("Commodity VegaCharge High")
        + col("Commodity CurvatureCharge High"))
}
/// Not a real measure. Used for analysis only
fn com_total_max(_: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        col("Commodity TotalCharge Low"),
        col("Commodity TotalCharge Medium"),
        col("Commodity TotalCharge High"),
    ]))
}

pub(crate) fn com_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "Commodity TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(com_total_low),
            depends_upon: vec![
                (
                    "Commodity DeltaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                ("Commodity VegaCharge Low".to_string(), "scalar".to_string()),
                (
                    "Commodity CurvatureCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "Commodity TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(com_total_medium),
            depends_upon: vec![
                (
                    "Commodity DeltaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity VegaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "Commodity TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(com_total_high),
            depends_upon: vec![
                (
                    "Commodity DeltaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity VegaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity CurvatureCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "Commodity TotalCharge MAX".to_string(),
            calculator: std::sync::Arc::new(com_total_max),
            depends_upon: vec![
                (
                    "Commodity TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
    ]
}
