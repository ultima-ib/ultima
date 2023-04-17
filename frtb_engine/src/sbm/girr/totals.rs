use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn girr_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("GIRR DeltaCharge Low") + col("GIRR VegaCharge Low") + col("GIRR CurvatureCharge Low"))
}

pub(crate) fn girr_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("GIRR DeltaCharge Medium")
        + col("GIRR VegaCharge Medium")
        + col("GIRR CurvatureCharge Medium"))
}

pub(crate) fn girr_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("GIRR DeltaCharge High")
        + col("GIRR VegaCharge High")
        + col("GIRR CurvatureCharge High"))
}

pub(crate) fn girr_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "GIRR TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(girr_total_low),
            depends_upon: vec![
                ("GIRR DeltaCharge Low".to_string(), "scalar".to_string()),
                ("GIRR VegaCharge Low".to_string(), "scalar".to_string()),
                ("GIRR CurvatureCharge Low".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "GIRR TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(girr_total_medium),
            depends_upon: vec![
                ("GIRR DeltaCharge Medium".to_string(), "scalar".to_string()),
                ("GIRR VegaCharge Medium".to_string(), "scalar".to_string()),
                (
                    "GIRR CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "GIRR TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(girr_total_high),
            depends_upon: vec![
                ("GIRR DeltaCharge High".to_string(), "scalar".to_string()),
                ("GIRR VegaCharge High".to_string(), "scalar".to_string()),
                (
                    "GIRR CurvatureCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
    ]
}
