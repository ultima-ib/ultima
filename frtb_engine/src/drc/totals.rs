use ultibi::polars::lazy::dsl::col;
use ultibi::polars::prelude::Expr;
use ultibi::{DependantMeasure, Measure, PolarsResult, CPM};

// TODO NOTE: add DRC Sec CTP - currently missing
pub(crate) fn drc_charge(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("DRC nonSec CapitalCharge") + col("DRC Sec nonCTP CapitalCharge"))
}

pub(crate) fn drc_total_measures() -> Vec<Measure> {
    vec![DependantMeasure {
        name: "DRC Charge".to_string(),
        calculator: std::sync::Arc::new(drc_charge),
        depends_upon: vec![
            ("DRC nonSec CapitalCharge".to_string(), "scalar".to_string()),
            (
                "DRC Sec nonCTP CapitalCharge".to_string(),
                "scalar".to_string(),
            ),
        ],
        calc_params: vec![],
    }
    .into()]
}
