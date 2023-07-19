mod common;
use common::*;
use ndarray::arr1;
use ultibi::DataSet;

#[test]
fn drc_nonsec() {
    let expected_res = arr1(&[
        10000.0,
        9527.777778,
        1429.166667,
        9527.777778,
        0.0,
        1429.166667,
        0.0,
        1.0,
        -10000.0,
        -5722.222222,
        0.0,
        0.0,
        -5722.222222,
        0.0,
        28.611111,
        0.0,
        5000.0,
        958.333333,
        2396.548886,
        10486.111111,
        -9527.777778,
        2496.388889,
        190.555556,
        0.523942,
    ]);

    let request = r#"
    {"measures": [
            ["DRC nonSec GrossJTD", "sum"],
            ["DRC nonSec GrossJTD Scaled", "sum"],
            ["DRC nonSec CapitalCharge", "scalar"],
            ["DRC nonSec NetLongJTD", "scalar"],
            ["DRC nonSec NetShortJTD", "scalar"],
            ["DRC nonSec NetLongJTD Weighted", "scalar"],
            ["DRC nonSec NetAbsShortJTD Weighted", "scalar"],
            ["DRC nonSec HBR", "scalar"]
                ],
        "groupby": ["Desk", "BucketBCBS"],
        "type": "AggregationRequest",
            "hide_zeros": false,
            "calc_params": {
                "jurisdiction": "BCBS",
                "apply_fx_curv_div": "true",
                "drc_offset": "false"
            }
        }
"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
#[cfg(feature = "CRR2")]
fn drc_nonsec_crr2() {
    let expected_res = arr1(&[
        10000.0,
        9527.777778,
        1429.166667,
        9527.777778,
        0.0,
        1429.166667,
        0.0,
        1.0,
        -10000.0,
        -5722.222222,
        0.0,
        0.0,
        -5722.222222,
        0.0,
        28.611111,
        0.0,
        5000.0,
        958.333333,
        2385.595555,
        10486.111111,
        -9527.777778,
        2410.555556,
        47.638889,
        0.523942,
    ]);

    let request = r#"
    {"measures": [
            ["DRC nonSec GrossJTD", "sum"],
            ["DRC nonSec GrossJTD Scaled", "sum"],
            ["DRC nonSec CapitalCharge", "scalar"],
            ["DRC nonSec NetLongJTD", "scalar"],
            ["DRC nonSec NetShortJTD", "scalar"],
            ["DRC nonSec NetLongJTD Weighted", "scalar"],
            ["DRC nonSec NetAbsShortJTD Weighted", "scalar"],
            ["DRC nonSec HBR", "scalar"]
                ],
        "groupby": ["Desk", "BucketBCBS"],
        "type": "AggregationRequest",
            "hide_zeros": false,
            "calc_params": {
                "jurisdiction": "CRR2",
                "apply_fx_curv_div": "true",
                "drc_offset": "false"
        }}
"#;
    assert_results(request, expected_res.sum(), None)
}

/// This is just testing overwrite functionality
/// Drc BCBS with this overwrite is equal to Drc CRR2
#[test]
fn drc_bcbs_with_overrides_eq_crr2() {
    let expected_res = arr1(&[3814.762221]);

    let request = r#"
    {   "filters": [],

        "groupby": ["RiskClass", "Desk"],
        
        "overrides": [{   "field": "SensWeights",
                          "value": "[0.005]",
                          "filters": [
                                    [{"op":"Eq", "field":"RiskClass", "value":"DRC_nonSec"}],
                                    [{"op":"Eq", "field":"CreditQuality", "value":"AA"}]
                                    ]
                    }],
        
        "measures": [
            ["DRC nonSec CapitalCharge", "scalar"]
                ],
        "type": "AggregationRequest",
        
        "hide_zeros": true,
        "calc_params": {
            "jurisdiction": "BCBS",
            "apply_fx_curv_div": "true",
            "drc_offset": "false"
        }}
"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
fn drc_charge() {
    use ultibi::{AggregationRequest, ComputeRequest};

    let request = r#"
    {"measures": [
        ["DRC Charge", "scalar"]
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

    assert!(dbg!(res.column("DRC Charge")).is_ok());
}
