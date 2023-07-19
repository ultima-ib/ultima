use polars::df;
use polars::prelude::NamedFrom;
use ultibi_core::{ComputeRequest, DataSet};

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
    let data_req = serde_json::from_str::<ComputeRequest>(req).expect("Could not parse request");

    let res = common::TEST_DASET.as_ref().compute(data_req).unwrap();

    let expected = df!(
        "State" => ["NY"],
        "Balance_sum" => [25.0]
    )
    .unwrap();

    assert_eq!(res, expected);
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

    let data_req = serde_json::from_str::<ComputeRequest>(req).expect("Could not parse request");

    common::TEST_DASET.as_ref().compute(data_req).unwrap();
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

    let data_req = serde_json::from_str::<ComputeRequest>(req).expect("Could not parse request");

    common::TEST_DASET.as_ref().compute(data_req).unwrap();
}
