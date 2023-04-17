use ultibi::polars::lazy::dsl::col;
use ultibi::polars::prelude::Expr;
use ultibi::{DependantMeasure, Measure, PolarsResult, CPM};

// TODO NOTE: add DRC Sec CTP - currently missing
fn sa_charge(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("SBM Charge") + col("DRC Charge") + col("RRAO Charge"))
}

pub(crate) fn sa_total_measures() -> Vec<Measure> {
    vec![Measure::Dependant(DependantMeasure {
        name: "SA Charge".to_string(),
        calculator: std::sync::Arc::new(sa_charge),
        depends_upon: vec![
            ("SBM Charge".to_string(), "scalar".to_string()),
            ("DRC Charge".to_string(), "scalar".to_string()),
            ("RRAO Charge".to_string(), "scalar".to_string()),
        ],
        calc_params: vec![],
    })]
}
