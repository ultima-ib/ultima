//! Main testing module for the DataSet and measures

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
    let a = (*common::TEST_DASET).as_ref();
    let res = a.compute(data_req).expect("Calculation failed");

    let res_sum = res
        .column("Balance_sum")
        .expect("Couldn't get column Balance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 25.0)
}
