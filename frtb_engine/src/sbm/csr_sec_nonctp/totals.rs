use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn csrsecnonctp_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec nonCTP DeltaCharge Low")
        + col("CSR Sec nonCTP VegaCharge Low")
        + col("CSR Sec nonCTP CurvatureCharge Low"))
}
pub(crate) fn csrsecnonctp_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec nonCTP DeltaCharge Medium")
        + col("CSR Sec nonCTP VegaCharge Medium")
        + col("CSR Sec nonCTP CurvatureCharge Medium"))
}
pub(crate) fn csrsecnonctp_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR Sec nonCTP DeltaCharge High")
        + col("CSR Sec nonCTP VegaCharge High")
        + col("CSR Sec nonCTP CurvatureCharge High"))
}

pub(crate) fn csrsecnonctp_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec nonCTP TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_total_low),
            depends_upon: vec![
                (
                    "CSR Sec nonCTP DeltaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP VegaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP CurvatureCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec nonCTP TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_total_medium),
            depends_upon: vec![
                (
                    "CSR Sec nonCTP DeltaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP VegaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR Sec nonCTP TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_total_high),
            depends_upon: vec![
                (
                    "CSR Sec nonCTP DeltaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP VegaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec nonCTP CurvatureCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
    ]
}
