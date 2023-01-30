//! Main testing module for the DataSet and measures

use base_engine::{AggregationRequest, execute_aggregation};
mod common;

//mod agg_req_tests {
//    #[cfg(feature = "cache")]
//    use base_engine::execution_with_cache::execute_with_cache;
//    use base_engine::{execute_aggregation, AggregationRequest};
//    use std::sync::Arc;
//
//    use crate::common;
//}

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
    let a = (*common::TEST_DASET).as_ref();
    let res = execute_aggregation(&data_req, a, false).expect("Calculation failed");

        let res_sum = res
            .column("Balance_sum")
            .expect("Couldn't get column Balance_sum")
            .sum::<f64>()
            .expect("Couldn't sum");
        assert_eq!(res_sum, 25.0)
}
/*
#[cfg(feature = "cache")]
#[test]
#[ignore]
fn simple_fltr_grpby_sum_with_cache() {
    use base_engine::execution_with_cache::get_cache_size;

    //     //     // TODO: is this req is ok?
    //     //     let raw_req = r#"
    //     // {"measures": [
    //     //     ["FX DeltaSens", "sum"],
    //     //     ["FX DeltaSens Weighted", "sum"],
    //     //     ["FX DeltaSb", "scalar"],
    //     //     ["FX DeltaKb", "scalar"],
    //     //     ["FX DeltaCharge Low", "scalar"],
    //     //     ["FX DeltaCharge Medium", "scalar"],
    //     //     ["FX DeltaCharge High", "scalar"],
    //     //     ["FX DeltaCharge MAX", "scalar"]
    //     //         ],
    //     // "groupby": ["Desk"],
    //     // "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    //     // "type": "AggregationRequest",

    //     // "calc_params": {"jurisdiction": "BCBS"}

    //     // }"#;

    let req = serde_json::from_str::<AggregationRequest>(raw_req).expect("Could not parse request");
    let dataset = (*common::TEST_DASET).as_ref();
    // TODO: find corect data for calculations
    let res1 = execute_with_cache(&req, dataset, false).expect("Calculation failed");
    assert_eq!(get_cache_size().unwrap(), 1);
    let res2 = execute_with_cache(&req, dataset, false).expect("Calculation failed");
    assert_eq!(get_cache_size().unwrap(), 1);

    //     //     let res_sum1 = res1
    //     //         .column("Balance_sum")
    //     //         .expect("Couldn't get column Balance_sum")
    //     //         .sum::<f64>()
    //     //         .expect("Couldn't sum");

    //     //     let res_sum2 = res2
    //     //         .column("Balance_sum")
    //     //         .expect("Couldn't get column Balance_sum")
    //     //         .sum::<f64>()
    //     //         .expect("Couldn't sum");

    //     //     assert_eq!(res_sum2, 25.0);
    //     //     assert_eq!(res_sum1, res_sum2);
    // }

    #[test]
    #[should_panic(expected = "missing field `groupby`")]
    fn missing_groupby() {
        let req = r#"{"measures": [["Balance", "sum"]], "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]}"#;
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
        let req = r#"{"measures": [], "groupby" : ["State"], "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]]}"#;
        let data_req =
            serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
        let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
            .expect("Calculation failed");
        assert!(res.is_empty())
    }
}
*/