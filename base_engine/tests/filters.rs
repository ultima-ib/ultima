use std::sync::Arc;

use base_engine::{execute_aggregation, AggregationRequest};

mod common;

#[test]
fn fltr_in_and_eq() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "In", "field": "City", "value": ["NY", "New York", "Forks"]}], [{"op": "Eq","field": "State", "value": "Washington"}]]            
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
        .expect("Calculation failed");

    let res_sum = res
        .column("Balance_sum")
        .expect("Couldn't get column Dalance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 20.0)
}

#[test]
fn fltr_eq_or_eq() {
    let req = r#"
    {"measures": [
        ["Balance", "mean"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "City", "value": "Sun Diego"}, {"op": "Eq", "field": "State", "value": "Washington"}], [{"op": "Eq", "field": "Sex", "value": "female"}]]            
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
        .expect("Calculation failed");

    let res_sum = res
        .column("Balance_mean")
        .expect("Couldn't get column Balance_mean")
        .mean()
        .expect("Couldn't find mean");
    assert_eq!(res_sum, 30.0)
}
