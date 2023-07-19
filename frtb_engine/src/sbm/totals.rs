//! Totals across different Risk Classes

use ultibi::polars::lazy::dsl::{col, max_horizontal, Expr};
use ultibi::{DependantMeasure, Measure, PolarsResult, CPM};

// Testing Dependant Measures
fn sbm_charge_low_dep(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge Low")
        + col("GIRR TotalCharge Low")
        + col("EQ TotalCharge Low")
        + col("CSR Sec nonCTP TotalCharge Low")
        + col("CSR nonSec TotalCharge Low")
        + col("CSR Sec CTP TotalCharge Low")
        + col("Commodity TotalCharge Low"))
}

fn sbm_charge_medium_dep(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge Medium")
        + col("GIRR TotalCharge Medium")
        + col("EQ TotalCharge Medium")
        + col("CSR Sec nonCTP TotalCharge Medium")
        + col("CSR nonSec TotalCharge Medium")
        + col("CSR Sec CTP TotalCharge Medium")
        + col("Commodity TotalCharge Medium"))
}
fn sbm_charge_high_dep(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("FX TotalCharge High")
        + col("GIRR TotalCharge High")
        + col("EQ TotalCharge High")
        + col("CSR Sec nonCTP TotalCharge High")
        + col("CSR nonSec TotalCharge High")
        + col("CSR Sec CTP TotalCharge High")
        + col("Commodity TotalCharge High"))
}

pub(crate) fn sbm_charge_dep(_: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        col("SBM Charge High"),
        col("SBM Charge Medium"),
        col("SBM Charge Low"),
    ]))
}

pub(crate) fn sbm_total_measures() -> Vec<Measure> {
    vec![
        // Testing dependency
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge Medium".to_string(),
            calculator: std::sync::Arc::new(sbm_charge_medium_dep),
            depends_upon: vec![
                ("FX TotalCharge Medium".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge Medium".to_string(), "scalar".to_string()),
                ("EQ TotalCharge Medium".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge Medium".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge Low".to_string(),
            calculator: std::sync::Arc::new(sbm_charge_low_dep),
            depends_upon: vec![
                ("FX TotalCharge Low".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge Low".to_string(), "scalar".to_string()),
                ("EQ TotalCharge Low".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge Low".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge High".to_string(),
            calculator: std::sync::Arc::new(sbm_charge_high_dep),
            depends_upon: vec![
                ("FX TotalCharge High".to_string(), "scalar".to_string()),
                ("GIRR TotalCharge High".to_string(), "scalar".to_string()),
                ("EQ TotalCharge High".to_string(), "scalar".to_string()),
                (
                    "CSR Sec nonCTP TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR nonSec TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "CSR Sec CTP TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
                (
                    "Commodity TotalCharge High".to_string(),
                    "scalar".to_string(),
                ),
            ],
            calc_params: vec![],
        }),
        Measure::Dependant(DependantMeasure {
            name: "SBM Charge".to_string(),
            calculator: std::sync::Arc::new(sbm_charge_dep),
            depends_upon: vec![
                ("SBM Charge Low".to_string(), "scalar".to_string()),
                ("SBM Charge Medium".to_string(), "scalar".to_string()),
                ("SBM Charge High".to_string(), "scalar".to_string()),
            ],
            calc_params: vec![],
        }),
    ]
}
