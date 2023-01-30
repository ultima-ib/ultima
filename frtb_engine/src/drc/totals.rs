use base_engine::polars::prelude::Expr;
use base_engine::{BaseMeasure, Measure, PolarsResult, CPM};

use super::drc_nonsec::drc_nonsec_charge;
use super::drc_secnonctp::drc_secnonctp_charge;

// TODO add DRC Sec CTP
pub(crate) fn drc_charge(op: &CPM) -> PolarsResult<Expr> {
    Ok(drc_nonsec_charge(op)? + drc_secnonctp_charge(op)?)
}

pub(crate) fn drc_total_measures() -> Vec<Measure> {
    vec![Measure::Base(BaseMeasure {
        name: "DRC Charge".to_string(),
        calculator: Box::new(drc_charge),
        aggregation: Some("scalar"),
        precomputefilter: None,
    })]
}
