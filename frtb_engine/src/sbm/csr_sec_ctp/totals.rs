use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn csrsecctp_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec CTP DeltaCharge Low")
        + col("CSR Sec CTP VegaCharge Low")
        + col("CSR Sec CTP CurvatureCharge Low"))
}
pub(crate) fn csrsecctp_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec CTP DeltaCharge Medium")
        + col("CSR Sec CTP VegaCharge Medium")
        + col("CSR Sec CTP CurvatureCharge Medium"))
}
pub(crate) fn csrsecctp_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec CTP DeltaCharge High")
        + col("CSR Sec CTP VegaCharge High")
        + col("CSR Sec CTP CurvatureCharge High"))
}

pub(crate) fn csrsecctp_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec CTP TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_total_low),
            depends_upon: vec![
                (
                    "CSR Sec CTP DeltaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP VegaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP CurvatureCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec CTP TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_total_medium),
            depends_upon: vec![
                (
                    "CSR Sec CTP DeltaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP VegaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec CTP TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_total_high),
            depends_upon: vec![
                (
                    "CSR Sec CTP DeltaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP VegaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP CurvatureCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
    ]
}
