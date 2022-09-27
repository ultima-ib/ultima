//! Main testing module for the DataSet and measures

use std::sync::Arc;

use base_engine::{AggregationRequest, execute_aggregation};

mod common;

#[test]
fn dataset1() {
    let req = r#"
    {"measures": [
        ["FX_DeltaSens", "sum"],
        ["FX_DeltaSens_Weighted", "sum"],
        ["FX_DeltaSb", "first"],
        ["FX_DeltaKb", "first"],
        ["FX_DeltaCharge_Low", "first"],
        ["FX_DeltaCharge_Medium", "first"],
        ["FX_DeltaCharge_High", "first"],
        ["FX_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    let data_req = serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let res = execute_aggregation(data_req, Arc::clone(&*common::TEST_DASET));
    
}