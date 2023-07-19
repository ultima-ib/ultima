use polars::prelude::*;
use ultibi::DependantMeasure;
use ultibi::Measure;
use ultibi::CPM;

pub(crate) fn csrnonsec_total_low(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR nonSec DeltaCharge Low")
        + col("CSR nonSec VegaCharge Low")
        + col("CSR nonSec CurvatureCharge Low"))
}
pub(crate) fn csrnonsec_total_medium(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR nonSec DeltaCharge Medium")
        + col("CSR nonSec VegaCharge Medium")
        + col("CSR nonSec CurvatureCharge Medium"))
}
pub(crate) fn csrnonsec_total_high(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("CSR nonSec DeltaCharge High")
        + col("CSR nonSec VegaCharge High")
        + col("CSR nonSec CurvatureCharge High"))
}

/// Not a real measure. Used for analysis only
fn csrnonsec_total_max(_: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        col("CSR nonSec TotalCharge Low"),
        col("CSR nonSec TotalCharge Medium"),
        col("CSR nonSec TotalCharge High"),
    ]))
}

pub(crate) fn csrnonsec_total_measures() -> Vec<Measure> {
    vec![
        Measure::Dependant(DependantMeasure {
            name: "CSR nonSec TotalCharge Low".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_total_low),
            depends_upon: vec![
                (
                    "CSR nonSec DeltaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec VegaCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec CurvatureCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR nonSec TotalCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_total_medium),
            depends_upon: vec![
                (
                    "CSR nonSec DeltaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec VegaCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec CurvatureCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR nonSec TotalCharge High".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_total_high),
            depends_upon: vec![
                (
                    "CSR nonSec DeltaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec VegaCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec CurvatureCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "CSR nonSec TotalCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_total_max),
            depends_upon: vec![
                (
                    "CSR nonSec TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
    ]
}
