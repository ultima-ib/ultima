use polars::df;
use polars::prelude::NamedFrom;
use ultibi_core::{ComputeRequest, DataSet};

mod common;

#[test]
fn with_override_sum() {
    let req = r#"
    {"measures": [
        ["Balance", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}, {"op": "Eq", "field": "City", "value": "Forks"}]],
    "overrides": [{   "field": "Balance",
                          "value": "100",
                          "filters": [
                                    [{"op":"Eq", "field":"State", "value":"NY"}],
                                    [{"op":"Eq", "field":"City", "value":"Buffalo"}]
                                    ]
                    }]        
    }"#;
    let data_req = serde_json::from_str::<ComputeRequest>(req).expect("Could not parse request");

    let res = common::TEST_DASET.as_ref().compute(data_req).unwrap();

    let expected = df!(
        "State" => ["NY", "Washington"],
        "Balance_sum" => [115.0, 20.0]
    )
    .unwrap();

    assert_eq!(res, expected);
}
