mod common;

mod tests {
    use base_engine::{execute_aggregation, AggregationRequest};
    use std::sync::Arc;

    use crate::common;
    #[test]
    fn add_row() {
        let req = r#"{"measures": [["Balance", "sum"]],
                            "groupby": ["State"],
                            "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
                            "add_row": {"prepare": true, "rows": [{"State": "NY", "Balance": "10"}, {"State": "NY", "Balance": "10"}]}}"#;
        let data_req =
            serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
        let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
            .expect("Calculation failed");

        let res_sum = dbg!(res)
            .column("Balance_sum")
            .expect("Couldn't get column Balance_sum")
            .sum::<f64>()
            .expect("Couldn't sum");
        assert_eq!(res_sum, 45.0)
    }

    #[test]
    /// Combining different schemas
    fn add_row2() {
        let req = r#"{"measures": [["Balance", "sum"]],
                            "groupby": ["State"],
                            "filters": [[{"op": "Eq", "field": "State", "value": "NY"}]],
                            "add_row": {"prepare": true, "rows": [{"State": "NY", "Balance": "10"}, {"State": "NY", "Age": "29"}]}}"#;
        let data_req =
            serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
        let res = execute_aggregation(&data_req, &*Arc::clone(&*common::TEST_DASET), false)
            .expect("Calculation failed");

        let res_sum = dbg!(res)
            .column("Balance_sum")
            .expect("Couldn't get column Balance_sum")
            .sum::<f64>()
            .expect("Couldn't sum");
        assert_eq!(res_sum, 35.0)
    }
}
