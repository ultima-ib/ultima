use std::sync::Arc;

use base_engine::{execute_aggregation, AggregationRequest};

mod common;

#[test]
fn simple_fltr_grpby_sum() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]         
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
    assert_eq!(res_sum, 25.0)
}

#[test]
#[should_panic(expected = "No measure NoSuchMeasure")]
fn non_existent_measure() {
    let req = r#"
    {"measures": [
        ["NoSuchMeasure", "sum"]
            ],
    "groupby": ["State"],
    "filters": []         
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let _ = dbg!(execute_aggregation(
        &data_req,
        &*Arc::clone(&*common::TEST_DASET),
        false
    ))
    .unwrap();
}

#[test]
#[should_panic(expected = "No action NoSuchAction")]
fn non_existent_action() {
    let req = r#"
    {"measures": [
        ["Balance", "NoSuchAction"]
            ],
    "groupby": ["State"],
    "filters": []         
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let _ = dbg!(execute_aggregation(
        &data_req,
        &*Arc::clone(&*common::TEST_DASET),
        false
    ))
    .unwrap();
}
