mod common;

use ultibi::DataSet;

#[test]
fn sa_charge() {
    use ultibi::{AggregationRequest, ComputeRequest};

    let request = r#"
    {"measures": [
        ["SA Charge", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
    
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    let data_req =
        serde_json::from_str::<AggregationRequest>(request).expect("Could not parse request");

    let compute_req = ComputeRequest::Aggregation(data_req);

    let res = common::LAZY_DASET.as_ref().compute(compute_req).unwrap();

    assert!(dbg!(res.column("SA Charge")).is_ok());
}
