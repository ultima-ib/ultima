use ultibi::{AggregationRequest, ComputeRequest, DataSet};
mod common;
use common::LAZY_DASET;
use ultibi::polars::prelude::{Float64Type, IndexOrder};

/// Note in later(post 25.1) versions of polars cannot call max_expr on
/// an aggregated Expr. See this:
/// https://github.com/pola-rs/polars/issues/6115
/// Hence if fails it's ok to drop this test
#[test]
fn dependant_sbm() {
    let request_as_dependants = r#"
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

    let req_deps = serde_json::from_str::<AggregationRequest>(request_as_dependants)
        .expect("Could not parse request");

    let a = &*LAZY_DASET;
    let mut res1 = a
        .compute(ComputeRequest::Aggregation(req_deps))
        .expect("Error while calculating dependant results");
    let _ = res1.drop_in_place("Desk").unwrap();
    let sum1 = res1
        .to_ndarray::<Float64Type>(IndexOrder::Fortran)
        .expect("Couldn't convert result 1 to ndarray")
        .sum();

    // This number is not derived from a Spread Sheet
    // Instead it was derived via measures which were derived from a SS
    let expected = 2267954.452798342;
    assert!((sum1 - expected).abs() < 1e-4);

    // ALso test performance! res2 must be much faster!
}
