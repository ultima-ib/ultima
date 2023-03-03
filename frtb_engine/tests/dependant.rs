use std::sync::Arc;

use base_engine::{exec_agg_base, AggregationRequest, DataSet, ComputeRequest};
mod common;
use common::LAZY_DASET;
use polars::prelude::Float64Type;

#[test]
fn single_level_dependant_sbm() {
    let request_basic = r#"
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

    let request_dependant = r#"
    {"measures": [
        ["SBM Charge High Dependant Test", "scalar"],
        ["SBM Charge Low Dependant Test", "scalar"],
        ["SBM Charge Medium Dependant Test", "scalar"],
        ["SBM Charge Dependant Test", "scalar"]
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
    let res1 = a.compute(ComputeRequest::Aggregation(req_basic), false)
        .expect("Error while calculating standard results");
    let res2 = a.compute(ComputeRequest::Aggregation(req_dep), false)
    .expect("Error while calculating results with dependants");
    let sum1 = res1.to_ndarray::<Float64Type>().expect("Couldn't convert result 1 to ndarray").sum();
    let sum2 = res2.to_ndarray::<Float64Type>().expect("Couldn't convert result 2 to ndarray").sum();
    assert!((sum1 - sum2).abs() < 1e-5);

    // ALso test performance! res2 must be much faster!
}
