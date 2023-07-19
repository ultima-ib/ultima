use ultibi_core::{
    cache::CacheableDataSet, AggregationRequest, CacheableAggregationRequest,
    CacheableComputeRequest, ComputeRequest, DataSet,
};

mod common;

// TODO test dependant is scalar agg
#[test]
fn agg_dependant_and_cache() {
    let req = r#"
    {"measures": [
        ["DivAge", "scalar"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
    "calc_params": {"count": "10"}      
    }"#;

    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let compute_req = ComputeRequest::Aggregation(data_req);

    let res = common::TEST_DASET_WITH_DEPENDANTS
        .as_ref()
        .compute(compute_req)
        .unwrap();

    let res_sum = res
        .column("DivAge")
        .expect("Couldn't get column Dalance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 3.1);

    let cached_req = r#"
    {"measure": ["Age", "sum"],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
    "calc_params": {"count": "10"}      
    }"#;

    let cr = serde_json::from_str::<CacheableAggregationRequest>(cached_req)
        .expect("Could not parse CacheableAggregationRequest");

    let ccr = CacheableComputeRequest::Aggregation(cr);

    assert!(common::TEST_DASET_WITH_DEPENDANTS
        .as_ref()
        .get_cache()
        .get(&ccr)
        .is_some());

    common::TEST_DASET_WITH_DEPENDANTS.clean_cache();

    assert!(common::TEST_DASET_WITH_DEPENDANTS
        .as_ref()
        .get_cache()
        .get(&ccr)
        .is_none());
}

#[test]
#[should_panic(expected = "Measure DivAge supports only scalar aggregation")]
fn dependant_is_scalar() {
    let req = r#"
    {"measures": [
        ["DivAge", "sum"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
    "calc_params": {"count": "10"}      
    }"#;

    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let compute_req = ComputeRequest::Aggregation(data_req);

    let res = common::TEST_DASET_WITH_DEPENDANTS
        .as_ref()
        .compute(compute_req)
        .unwrap();

    let res_sum = res
        .column("DivAge")
        .expect("Couldn't get column Dalance_sum")
        .sum::<f64>()
        .expect("Couldn't sum");
    assert_eq!(res_sum, 3.1);
}

#[test]
#[should_panic(expected = "No measure NoSuchMeasure exists for the dataset")]
fn child_not_found() {
    let req = r#"
    {"measures": [
        ["NoSuchMeasureTest", "scalar"]
            ],
    "groupby": ["State"],
    "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
    "calc_params": {"count": "10"}      
    }"#;

    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let compute_req = ComputeRequest::Aggregation(data_req);

    let _ = common::TEST_DASET_WITH_DEPENDANTS
        .as_ref()
        .compute(compute_req)
        .unwrap();
}
