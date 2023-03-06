use ultibi::{AggregationRequest, ComputeRequest, DataSet};
mod common;
use common::LAZY_DASET;
use polars::prelude::Float64Type;

/// Note in later(post 25.1) versions of polars cannot call max_expr on
/// an aggregated Expr. See this:
/// https://github.com/pola-rs/polars/issues/6115
/// Hence if fails it's ok to drop this test
#[test]
fn dependant_sbm() {
    let request_basic = r#"
    {"measures": [
        ["SBM Charge High Test", "scalar"],
        ["SBM Charge Low Test", "scalar"],
        ["SBM Charge Medium Test", "scalar"],
        ["SBM Charge Test", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "type": "AggregationRequest",

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;

    let request_dependant = r#"
    {"measures": [
        ["SBM Charge High", "scalar"],
        ["SBM Charge Low", "scalar"],
        ["SBM Charge Medium", "scalar"],
        ["SBM Charge", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "type": "AggregationRequest",

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;

    let req_basic =
        serde_json::from_str::<AggregationRequest>(request_basic).expect("Could not parse request");
    let req_dep = serde_json::from_str::<AggregationRequest>(request_dependant)
        .expect("Could not parse request");

    let a = &*LAZY_DASET;
    let mut res1 = a
        .compute(ComputeRequest::Aggregation(req_basic), false)
        .expect("Error while calculating standard results");
    let mut res2 = a
        .compute(ComputeRequest::Aggregation(req_dep), false)
        .expect("Error while calculating results with dependants");

    let _ = res1.drop_in_place("Desk").unwrap();
    let _ = res2.drop_in_place("Desk").unwrap();

    let sum1 = res1
        .to_ndarray::<Float64Type>()
        .expect("Couldn't convert result 1 to ndarray")
        .sum();
    let sum2 = res2
        .to_ndarray::<Float64Type>()
        .expect("Couldn't convert result 2 to ndarray")
        .sum();
    assert!((sum1 - sum2).abs() < 1e-4);

    // ALso test performance! res2 must be much faster!
}
