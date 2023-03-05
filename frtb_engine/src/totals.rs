use base_engine::polars::prelude::Expr;
use base_engine::{Measure, PolarsResult, CPM, DependantMeasure};
use base_engine::polars::lazy::dsl::col;

use crate::drc::totals::drc_charge;
use crate::rrao::rrao_charge;
use crate::sbm::totals::sbm_charge;

// TODO add DRC Sec CTP
fn _sa_charge(op: &CPM) -> PolarsResult<Expr> {
    Ok(sbm_charge(op)? + drc_charge(op)? + rrao_charge(op)?)
}

// TODO add DRC Sec CTP
fn sa_charge(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("SBM Charge") + col("DRC Charge") + col("RRAO Charge"))
}

pub(crate) fn sa_total_measures() -> Vec<Measure> {
    vec![
    //Measure::Base(BaseMeasure {
    //    name: "SA Charge".to_string(),
    //    calculator: Box::new(_sa_charge),
    //    aggregation: Some("scalar"),
    //    precomputefilter: None,
    //}),
    
    Measure::Dependant(DependantMeasure {
        name: "SA Charge".to_string(),
        calculator: Box::new(sa_charge),
        depends_upon: vec![
            ("SBM Charge".to_string(), "scalar".to_string()),
            ("DRC Charge".to_string(), "scalar".to_string()),
            ("RRAO Charge".to_string(), "scalar".to_string()),
        ],
    }),
    ]
}
