use std::sync::Arc;

use base_engine::{execute_aggregation, AggregationRequest};
mod common;
use common::LAZY_DASET;

#[test]
#[ignore]
fn dependant_sbm() {
    let request_basic = r#"
    {"measures": [
        ["SBM Charge", "scalar"],
        ["SBM Charge High", "scalar"],
        ["SBM Charge Low", "scalar"],
        ["SBM Charge Medium", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "type": "AggregationRequest",

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;

    let request_dependant = r#"
    {"measures": [
        ["SBM Charge Dependant Test", "scalar"],
        ["SBM Charge High Dependant Test", "scalar"],
        ["SBM Charge Low Dependant Test", "scalar"],
        ["SBM Charge Medium Dependant Test", "scalar"]
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
    let res1 = execute_aggregation(&req_basic, &*Arc::clone(a), false)
        .expect("Error while calculating results");
    let res2 = execute_aggregation(&req_dep, &*Arc::clone(a), false)
        .expect("Error while calculating results");
    assert_eq!(res1, res2);

    // ALso test performance! res2 must be much faster!
}
