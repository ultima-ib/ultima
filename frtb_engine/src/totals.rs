use base_engine::polars::prelude::Expr;
use base_engine::{Measure, OCP};

use crate::drc::totals::drc_charge;
use crate::rrao::rrao_charge;
use crate::sbm::totals::sbm_charge;

// TODO add DRC Sec CTP
fn sa_charge(op: &OCP) -> Expr {
    sbm_charge(op) + drc_charge(op) + rrao_charge(op)
}

pub(crate) fn sa_total_measures() -> Vec<Measure> {
    vec![Measure {
        name: "SA Charge".to_string(),
        calculator: Box::new(sa_charge),
        aggregation: Some("scalar"),
        precomputefilter: None,
    }]
}
