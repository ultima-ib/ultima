use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn csrsecctp_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(
        col("CSR secCTP DeltaCharge Low")
        + col("CSR secCTP VegaCharge Low")
        + col("CSR secCTP CurvatureCharge Low")
    )
}
pub(crate) fn csrsecctp_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(
        col("CSR secCTP DeltaCharge Medium")
        + col("CSR secCTP VegaCharge Medium")
        + col("CSR secCTP CurvatureCharge Medium")
    )
}
pub(crate) fn csrsecctp_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(
        col("CSR secCTP DeltaCharge High")
        + col("CSR secCTP VegaCharge High")
        + col("CSR secCTP CurvatureCharge High")
    )
}

pub(crate) fn csrsecctp_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "CSR secCTP TotalCharge Low".to_string(),
            calculator: Box::new(csrsecctp_total_low),
            depends_upon: vec![
                ("CSR secCTP DeltaCharge Low".to_string(), "scalar".to_string()),
                ("CSR secCTP VegaCharge Low".to_string(), "scalar".to_string()),
                ("CSR secCTP CurvatureCharge Low".to_string(), "scalar".to_string())
            ],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR secCTP TotalCharge Medium".to_string(),
            calculator: Box::new(csrsecctp_total_medium),
            depends_upon: vec![
                ("CSR secCTP DeltaCharge Medium".to_string(), "scalar".to_string()),
                ("CSR secCTP VegaCharge Medium".to_string(), "scalar".to_string()),
                ("CSR secCTP CurvatureCharge Medium".to_string(), "scalar".to_string())
            ],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR secCTP TotalCharge High".to_string(),
            calculator: Box::new(csrsecctp_total_high),
            depends_upon: vec![
                ("CSR secCTP DeltaCharge High".to_string(), "scalar".to_string()),
                ("CSR secCTP VegaCharge High".to_string(), "scalar".to_string()),
                ("CSR secCTP CurvatureCharge High".to_string(), "scalar".to_string())
            ],
        }),
    ]
}
