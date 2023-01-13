//! Main testing module for the DataSet and measures

#[cfg(feature = "cache")]
use base_engine::execution_with_cache::execute_with_cache;
use base_engine::{execute_aggregation, AggregationRequest};
use std::sync::Arc;

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
    let a = (&*common::TEST_DASET).as_ref();
    let res = execute_aggregation(&data_req, a, false).expect("Calculation failed");

    let res_sum = res
        .column("Balance_sum")
        .expect("Couldn't get column Balance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 25.0)
}

#[cfg(feature = "cache")]
#[test]
fn simple_fltr_grpby_sum_with_cache() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]         
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let a = (&*common::TEST_DASET).as_ref();
    let res1 = execute_with_cache(&data_req, a, false).expect("Calculation failed");
    let res2 = execute_with_cache(&data_req, a, false).expect("Calculation failed");

    let res_sum1 = res1
        .column("Balance_sum")
        .expect("Couldn't get column Balance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");

    let res_sum2 = res2
        .column("Balance_sum")
        .expect("Couldn't get column Balance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");

    assert_eq!(res_sum1, 25.0);
    assert_eq!(res_sum2, 25.0);
    assert_eq!(res_sum1, res_sum2);
}

#[test]
#[should_panic(expected = "missing field `groupby`")]
fn missing_groupby() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
            "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]         
        }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let _res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
        .expect("Calculation failed");
}

#[cfg(feature = "cache")]
#[test]
#[should_panic(expected = "expected keys in groupby operation, got nothing")]
fn empty_groupby() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby" : [],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]         
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let _res = execute_with_cache(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
        .expect("Calculation failed");
}

#[test]
#[should_panic(expected = "Select measures")]
fn empty_measures() {
    let req = r#"
    {"measures": [            ],
    "groupby" : ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]         
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
        .expect("Calculation failed");
    assert!(res.is_empty())
}
