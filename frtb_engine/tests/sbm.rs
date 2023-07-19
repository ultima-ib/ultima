mod common;

use common::*;
use ndarray::{arr1, arr2};

#[test]
fn fx_delta() {
    let expected_res = arr1(&[
        115.0, 12.197592, 12.197592, 12.197592, 11.652789, 11.803866, 11.953033, 11.953033,
    ]);
    let request = r#"
    {"measures": [
        ["FX DeltaSens", "sum"],
        ["FX DeltaSens Weighted", "sum"],
        ["FX DeltaSb", "scalar"],
        ["FX DeltaKb", "scalar"],
        ["FX DeltaCharge Low", "scalar"],
        ["FX DeltaCharge Medium", "scalar"],
        ["FX DeltaCharge High", "scalar"],
        ["FX DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
    
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
#[cfg(feature = "CRR2")]
fn fx_delta_crr2() {
    let expected_res = arr1(&[
        115.0, 1.59099, 1.59099, 1.59099, 1.59099, 1.59099, 1.59099, 1.59099,
    ]);
    let request = r#"
    {"measures": [
        ["FX DeltaSens", "sum"],
        ["FX DeltaSens Weighted", "sum"],
        ["FX DeltaSb", "scalar"],
        ["FX DeltaKb", "scalar"],
        ["FX DeltaCharge Low", "scalar"],
        ["FX DeltaCharge Medium", "scalar"],
        ["FX DeltaCharge High", "scalar"],
        ["FX DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
    
    "calc_params": {"jurisdiction": "CRR2"}
            
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn fx_vega() {
    let expected_res = arr1(&[
        53000.0,
        53000.0,
        53000.0,
        50894.787649,
        51958.261414,
        53000.0,
        49423.256786,
        50875.624376,
        52287.6658,
        52287.6658,
    ]);
    let request = r#"
    {"measures": [
        ["FX VegaSens", "sum"],
        ["FX VegaSens Weighted", "sum"],
        ["FX VegaSb", "scalar"],
        ["FX VegaKb Low", "scalar"],
        ["FX VegaKb Medium", "scalar"],
        ["FX VegaKb High", "scalar"],
        ["FX VegaCharge Low", "scalar"],
        ["FX VegaCharge Medium", "scalar"],
        ["FX VegaCharge High", "scalar"],
        ["FX VegaCharge MAX", "scalar"]

            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn fx_curvature() {
    let expected_res = arr1(&[
        369000.0,
        39138.360339,
        3000.0,
        -3000.0,
        28107.613597,
        -28107.613597,
        28107.613597,
        0.0,
        28107.613597,
        28107.613597,
        23550.772425,
        24159.070424,
        24752.423835,
        24752.423835,
    ]);
    let request = r#"
    {"measures": [
        ["FX CurvatureDelta", "sum"],
["FX CurvatureDelta Weighted", "sum"],
["FX PnLup", "sum"],
["FX PnLdown", "sum"],
["FX CVRup", "sum"],
["FX CVRdown", "sum"],
["FX Curvature KbPlus", "scalar"],
["FX Curvature KbMinus", "scalar"],
["FX Curvature Kb", "scalar"],
["FX Curvature Sb", "scalar"],
["FX CurvatureCharge Low", "scalar"],
["FX CurvatureCharge Medium", "scalar"],
["FX CurvatureCharge High", "scalar"],
["FX CurvatureCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "RatesEM"}]],
    "type": "AggregationRequest",
    "hide_zeros": true,
    "calc_params": {"jurisdiction": "BCBS",
                "apply_fx_curv_div": "true"}
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}
#[test]
fn fx_total() {
    let expected_res = arr1(&[
        11.652789 + 49423.256786,
        11.803866 + 50875.624376,
        11.953033 + 52287.6658,
    ]);
    let request = r#"
    {"measures": [
        ["FX TotalCharge Low", "scalar"],
["FX TotalCharge Medium", "scalar"],
["FX TotalCharge High", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn girr_delta() {
    let expected_res = arr1(&[
        2581.0, 35.925432, 35.925432, 28.072696, 29.023816, 29.941875, 26.770639, 28.0824,
        29.335659, 29.335659,
    ]);
    let request = r#"
    {"measures": [
        ["GIRR DeltaSens", "sum"],
["GIRR DeltaSens Weighted", "sum"],
["GIRR DeltaSb", "scalar"],
["GIRR DeltaKb Low", "scalar"],
["GIRR DeltaKb Medium", "scalar"],
["GIRR DeltaKb High", "scalar"],
["GIRR DeltaCharge Low", "scalar"],
["GIRR DeltaCharge Medium", "scalar"],
["GIRR DeltaCharge High", "scalar"],
["GIRR DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
     "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, dbg!(expected_res).sum(), Some(1e-4))
}

#[test]
fn girr_vega() {
    let expected_res = arr1(&[
        210000.0,
        210000.0,
        210000.0,
        157611.879405,
        163407.920578,
        169005.3031,
        143838.458921,
        156128.390288,
        167519.092174,
        167519.092174,
    ]);
    let request = r#"
    {"measures": [
        ["GIRR VegaSens", "sum"],
        ["GIRR VegaSens Weighted", "sum"],
        ["GIRR VegaSb", "scalar"],
        ["GIRR VegaKb Low", "scalar"],
        ["GIRR VegaKb Medium", "scalar"],
        ["GIRR VegaKb High", "scalar"],
        ["GIRR VegaCharge Low", "scalar"],
        ["GIRR VegaCharge Medium", "scalar"],
        ["GIRR VegaCharge High", "scalar"],
        ["GIRR VegaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn girr_curvature() {
    let expected_res = arr2(&[
        [
            270000.0,
            22000.0,
            -15000.0,
            3992.497834,
            -18007.502166,
            11007.502166,
            0.0,
            11007.502166,
            11007.502166,
            11007.502166,
            8567.192327,
            8779.024461,
            8985.864266,
            8985.864266,
        ],
        [
            270000.0,
            22000.0,
            -15000.0,
            3992.497834,
            -18007.502166,
            11007.502166,
            0.0,
            11007.502166,
            11007.502166,
            11007.502166,
            8567.192327,
            8779.024461,
            8985.864266,
            8985.864266,
        ],
    ]);
    let request = r#"
    {"measures": [
        ["GIRR CurvatureDelta", "sum"],
["GIRR PnLup", "sum"],
["GIRR PnLdown", "sum"],
["GIRR CurvatureDelta Weighted", "sum"],
["GIRR CVRup", "sum"],
["GIRR CVRdown", "sum"],
["GIRR Curvature KbPlus", "scalar"],
["GIRR Curvature KbMinus", "scalar"],
["GIRR Curvature Kb", "scalar"],
["GIRR Curvature Sb", "scalar"],
["GIRR CurvatureCharge Low", "scalar"],
["GIRR CurvatureCharge Medium", "scalar"],
["GIRR CurvatureCharge High", "scalar"],
["GIRR CurvatureCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn girr_totals() {
    let expected_res = arr1(&[
        26.770639,
        28.0824,
        29.335659,
        143838.458921,
        156128.390288,
        167519.092174,
        0.0,
        0.0,
        0.0,
    ]);
    let request = r#"
    {"measures": [
        ["GIRR TotalCharge Low", "scalar"],
["GIRR TotalCharge Medium", "scalar"],
["GIRR TotalCharge High", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn eq_delta() {
    let expected_res = arr1(&[
        2800.0,
        1089.0,
        1089.0,
        987.273493,
        995.758659,
        1004.116547,
        665.011398,
        683.999424,
        702.474388,
        702.474388,
    ]);
    let request = r#"
    {"measures": [
        ["EQ DeltaSens", "sum"],
["EQ DeltaSens Weighted", "sum"],
["EQ DeltaSb", "scalar"],
["EQ DeltaKb Low", "scalar"],
["EQ DeltaKb Medium", "scalar"],
["EQ DeltaKb High", "scalar"],
["EQ DeltaCharge Low", "scalar"],
["EQ DeltaCharge Medium", "scalar"],
["EQ DeltaCharge High", "scalar"],
["EQ DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "hide_zeros":true,
    "calc_params": {"jurisdiction": "CRR2"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
fn eq_vega() {
    let expected_res = arr1(&[
        60000.0,
        55556.349186,
        55556.349186,
        46233.467601,
        46669.280435,
        47098.051013,
        28620.521491,
        29224.393971,
        29816.038563,
        29816.038563,
    ]);
    let request = r#"
    {"measures": [
        ["EQ VegaSens", "sum"],
        ["EQ VegaSens Weighted", "sum"],
        ["EQ VegaSb", "scalar"],
        ["EQ VegaKb Low", "scalar"],
        ["EQ VegaKb Medium", "scalar"],
        ["EQ VegaKb High", "scalar"],
        ["EQ VegaCharge Low", "scalar"],
        ["EQ VegaCharge Medium", "scalar"],
        ["EQ VegaCharge High", "scalar"],
        ["EQ VegaCharge MAX", "scalar"]
    ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
            "hide_zeros": true,
        "calc_params": {"jurisdiction": "BCBS",
        "apply_fx_curv_div": "true"}
        
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn eq_curv() {
    let expected_res = arr1(&[19559.580453, 19778.428906, 19994.882158, 19994.882158]);
    let request = r#"
    {"measures": [
        ["EQ CurvatureCharge Low", "scalar"],
        ["EQ CurvatureCharge Medium", "scalar"],
        ["EQ CurvatureCharge High", "scalar"],
        ["EQ CurvatureCharge MAX", "scalar"]
    ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "RatesEM"}]],
    "type": "AggregationRequest",
        "hide_zeros": true,
    "calc_params": {"jurisdiction": "BCBS"}
        
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn eq_totals() {
    let expected_res = arr1(&[
        665.011398 + 28620.521491 + 0.,
        683.999424 + 29224.393971 + 0.,
        702.474388 + 29816.038563 + 0.,
    ]);
    let request = r#"
    {"measures": [
        ["EQ TotalCharge Low", "scalar"],
["EQ TotalCharge Medium", "scalar"],
["EQ TotalCharge High", "scalar"]
    ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "hide_zeros": true,
    "calc_params": {"jurisdiction": "BCBS"}    
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn csr_nonsec_bcbs_delta() {
    let expected_res = arr1(&[
        45000.0, 975.0, 975.0, 684.920009, 768.283274, 843.4428, 656.018202, 742.954861,
        820.733799, 820.733799,
    ]);
    let request = r#"
    {"measures": [
        ["CSR nonSec DeltaSens", "sum"],
["CSR nonSec DeltaSens Weighted", "sum"],
["CSR nonSec DeltaSb", "scalar"],
["CSR nonSec DeltaKb Low", "scalar"],
["CSR nonSec DeltaKb Medium", "scalar"],
["CSR nonSec DeltaKb High", "scalar"],
            ["CSR nonSec DeltaCharge Low", "scalar"],
["CSR nonSec DeltaCharge Medium", "scalar"],
["CSR nonSec DeltaCharge High", "scalar"],
["CSR nonSec DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
#[cfg(feature = "CRR2")]
fn csr_nonsec_crr2_delta() {
    let expected_res = arr1(&[
        45000.0,
        1950.0,
        1950.0,
        1896.229439,
        1907.3086,
        1917.346389,
        1804.405734,
        1804.718141,
        1805.030495,
        1805.030495,
    ]);
    let request = r#"
    {"measures": [
        ["CSR nonSec DeltaSens", "sum"],
["CSR nonSec DeltaSens Weighted", "sum"],
["CSR nonSec DeltaSb", "scalar"],
["CSR nonSec DeltaKb Low", "scalar"],
["CSR nonSec DeltaKb Medium", "scalar"],
["CSR nonSec DeltaKb High", "scalar"],
            ["CSR nonSec DeltaCharge Low", "scalar"],
["CSR nonSec DeltaCharge Medium", "scalar"],
["CSR nonSec DeltaCharge High", "scalar"],
["CSR nonSec DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",
        "hide_zeros":true,
    "calc_params": {"jurisdiction": "CRR2"}
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn csr_nonsec_bcbs_vega() {
    let expected_res = arr1(&[
        33000.0,
        33000.0,
        33000.0,
        25549.287697,
        26896.533904,
        28167.670869,
        20743.2964,
        23075.707625,
        25193.098585,
    ]);
    let request = r#"
    {"measures": [ 
        ["CSR nonSec VegaSens", "sum"],
        ["CSR nonSec VegaSens Weighted", "sum"],
        ["CSR nonSec VegaSb", "scalar"],
        ["CSR nonSec VegaKb Low", "scalar"],
        ["CSR nonSec VegaKb Medium", "scalar"],
        ["CSR nonSec VegaKb High", "scalar"],
        ["CSR nonSec VegaCharge Low", "scalar"],
        ["CSR nonSec VegaCharge Medium", "scalar"],
        ["CSR nonSec VegaCharge High", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "Rates"}]],
    "type": "AggregationRequest",
        "hide_zeros": true,
    "calc_params": {"jurisdiction": "BCBS"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
#[cfg(feature = "CRR2")]
fn csr_nonsec_crr2_vega() {
    let expected_res = arr1(&[
        33000.0,
        33000.0,
        33000.0,
        25149.920952,
        26389.402294,
        27561.245693,
        22502.1728,
        23106.410208,
        23695.244337,
    ]);
    let request = r#"
    {"measures": [ 
        ["CSR nonSec VegaSens", "sum"],
        ["CSR nonSec VegaSens Weighted", "sum"],
        ["CSR nonSec VegaSb", "scalar"],
        ["CSR nonSec VegaKb Low", "scalar"],
        ["CSR nonSec VegaKb Medium", "scalar"],
        ["CSR nonSec VegaKb High", "scalar"],
        ["CSR nonSec VegaCharge Low", "scalar"],
        ["CSR nonSec VegaCharge Medium", "scalar"],
        ["CSR nonSec VegaCharge High", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "Rates"}]],
    "type": "AggregationRequest",
        "hide_zeros": true,
    "calc_params": {"jurisdiction": "CRR2"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn commodity_delta() {
    let expected_res = arr1(&[
        -250.0, -122.5, -122.5, 408.934179, 405.736564, 402.5, 269.704639, 260.4533, 250.861017,
        269.704639,
    ]);
    let request = r#"
    {"measures": [
        ["Commodity DeltaSens", "sum"],
        ["Commodity DeltaSens Weighted", "sum"],
        ["Commodity DeltaSb", "scalar"],
        ["Commodity DeltaKb Low", "scalar"],
        ["Commodity DeltaKb Medium", "scalar"],
        ["Commodity DeltaKb High", "scalar"],
        ["Commodity DeltaCharge Low", "scalar"],
        ["Commodity DeltaCharge Medium", "scalar"],
        ["Commodity DeltaCharge High", "scalar"],
        ["Commodity DeltaCharge MAX", "scalar"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "type": "AggregationRequest",

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
#[should_panic(expected = "RiskClass")]
/// User sets prepare to true but doesn't provide all columns required by prepare
fn add_rows_prepare_no_bucket() {
    let expected_res = arr1(&[0.]);
    let request = r#"
    {"measures": [
        ["Commodity DeltaSens", "sum"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "add_row": {"prepare": true, "rows": [
        {
            "SensitivitySpot": "1000000"}
        ]},  

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
/// User sets prepare to true but none of the required columns matches weights assignments
/// so the weight of this sensi should be [0.0]
fn add_rows_nothing_to_match_prepare() {
    let expected_res = arr1(&[-250.0]);
    let request = r#"
    {"measures": [
        ["Commodity DeltaSens", "sum"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "add_row": {"prepare": true, "rows": [
        {
            "SensitivitySpot": "1000000",
            "PnL_Up": "0",
            "PnL_Down": "0",
            "COB": "Test",
            "MaturityDate": "Test",
            "RiskClass": "Test",
            "RiskFactor": "Test",
            "RiskCategory": "Test",
            "RiskFactorType": "Test",
            "BucketBCBS": "Test",
            "BucketCRR2": "Test",
            "CreditQuality": "Test",
            "CoveredBondReducedWeight": "Test"}
        ]},  

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}

#[test]
#[should_panic(expected = "could not be parsed")]
/// PnL_Up must be a number
fn add_rows_bad_format() {
    let expected_res = arr1(&[0.]);
    let request = r#"
    {"measures": [
        ["Commodity DeltaSens", "sum"]
            ],
    "groupby": ["Desk"],
    "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
    "add_row": {"prepare": true, "rows": [
        {
            "SensitivitySpot": "1000000",
            "PnL_Up": "Test"}
        ]},  

    "hide_zeros":true,
    "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}
