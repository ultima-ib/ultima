//! Main testing module for the DataSet and measures

use std::sync::Arc;

use base_engine::{AggregationRequest, execute_aggregation};

mod common;


#[test]
fn simple_fltr_grpby_sum() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"Eq": ["State", "NY"]}]]            
    }"#;
    let data_req = serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let res = execute_aggregation(data_req, Arc::clone(&*common::TEST_DASET)).expect("Calculation failed");

    let res_sum = res.column("Balance_sum")
        .expect("Couldn't get column Dalance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 25.0)    
}
