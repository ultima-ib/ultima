mod common;

use common::*;
use ndarray::arr1;

#[test]
fn rrao() {
    let expected_res = arr1(&[20.0, 1.0, 21.0]);

    let request = r#"
    {"measures": [
        ["Exotic_RRAO_Charge", "sum"],
        ["Other_RRAO_Charge", "sum"],
        ["RRAO_Charge", "first"]
                ],
        "groupby": ["COB"],
        "type": "AggregationRequest"
    }
"#;
    assert_results(request, expected_res.sum(), None)
}
