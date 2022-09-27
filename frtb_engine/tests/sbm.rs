mod common;

use ndarray::prelude::*;
use common::*;

#[test]
fn fx_delta() {
    let expected_res = arr1(&[
        115.0, 12.197592, 12.197592, 12.197592, 11.652789, 11.803866, 11.953033,
        11.953033,
    ]);
    let request = r#"
    {"measures": [
        ["FX_DeltaSens", "sum"],
        ["FX_DeltaSens_Weighted", "sum"],
        ["FX_DeltaSb", "first"],
        ["FX_DeltaKb", "first"],
        ["FX_DeltaCharge_Low", "first"],
        ["FX_DeltaCharge_Medium", "first"],
        ["FX_DeltaCharge_High", "first"],
        ["FX_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    
    "calc_params": {"jurisdiction": "BCBS"}
            
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
        ["FX_VegaSens", "sum"],
        ["FX_VegaSens_Weighted", "sum"],
        ["FX_VegaSb", "first"],
        ["FX_VegaKb_Low", "first"],
        ["FX_VegaKb_Medium", "first"],
        ["FX_VegaKb_High", "first"],
        ["FX_VegaCharge_Low", "first"],
        ["FX_VegaCharge_Medium", "first"],
        ["FX_VegaCharge_High", "first"],
        ["FX_VegaCharge_MAX", "first"]

            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["FX_CurvatureDelta", "sum"],
["FX_CurvatureDelta_Weighted", "sum"],
["FX_PnLup", "sum"],
["FX_PnLdown", "sum"],
["FX_CVRup", "sum"],
["FX_CVRdown", "sum"],
["FX_Curvature_KbPlus", "first"],
["FX_Curvature_KbMinus", "first"],
["FX_Curvature_Kb", "first"],
["FX_Curvature_Sb", "first"],
["FX_CurvatureCharge_Low", "first"],
["FX_CurvatureCharge_Medium", "first"],
["FX_CurvatureCharge_High", "first"],
["FX_CurvatureCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "RatesEM"]]}],
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
        ["FX_TotalCharge_Low", "first"],
["FX_TotalCharge_Medium", "first"],
["FX_TotalCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["GIRR_DeltaSens", "sum"],
["GIRR_DeltaSens_Weighted", "sum"],
["GIRR_DeltaSb", "first"],
["GIRR_DeltaKb_Low", "first"],
["GIRR_DeltaKb_Medium", "first"],
["GIRR_DeltaKb_High", "first"],
["GIRR_DeltaCharge_Low", "first"],
["GIRR_DeltaCharge_Medium", "first"],
["GIRR_DeltaCharge_High", "first"],
["GIRR_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["GIRR_VegaSens", "sum"],
        ["GIRR_VegaSens_Weighted", "sum"],
        ["GIRR_VegaSb", "first"],
        ["GIRR_VegaKb_Low", "first"],
        ["GIRR_VegaKb_Medium", "first"],
        ["GIRR_VegaKb_High", "first"],
        ["GIRR_VegaCharge_Low", "first"],
        ["GIRR_VegaCharge_Medium", "first"],
        ["GIRR_VegaCharge_High", "first"],
        ["GIRR_VegaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["GIRR_CurvatureDelta", "sum"],
["GIRR_PnLup", "sum"],
["GIRR_PnLdown", "sum"],
["GIRR_CurvatureDelta_Weighted", "sum"],
["GIRR_CVRup", "sum"],
["GIRR_CVRdown", "sum"],
["GIRR_Curvature_KbPlus", "first"],
["GIRR_Curvature_KbMinus", "first"],
["GIRR_Curvature_Kb", "first"],
["GIRR_Curvature_Sb", "first"],
["GIRR_CurvatureCharge_Low", "first"],
["GIRR_CurvatureCharge_Medium", "first"],
["GIRR_CurvatureCharge_High", "first"],
["GIRR_CurvatureCharge_MAX", "first"]
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
        26.770639, 28.0824, 29.335659, 143838.458921, 156128.390288, 167519.092174, 0.0, 0.0, 0.0
    ]);
    let request = r#"
    {"measures": [
        ["GIRR_TotalCharge_Low", "first"],
["GIRR_TotalCharge_Medium", "first"],
["GIRR_TotalCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["EQ_DeltaSens", "sum"],
["EQ_DeltaSens_Weighted", "sum"],
["EQ_DeltaSb", "first"],
["EQ_DeltaKb_Low", "first"],
["EQ_DeltaKb_Medium", "first"],
["EQ_DeltaKb_High", "first"],
["EQ_DeltaCharge_Low", "first"],
["EQ_DeltaCharge_Medium", "first"],
["EQ_DeltaCharge_High", "first"],
["EQ_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        29816.038563
    ]);
    let request = r#"
    {"measures": [
        ["EQ_VegaSens", "sum"],
        ["EQ_VegaSens_Weighted", "sum"],
        ["EQ_VegaSb", "first"],
        ["EQ_VegaKb_Low", "first"],
        ["EQ_VegaKb_Medium", "first"],
        ["EQ_VegaKb_High", "first"],
        ["EQ_VegaCharge_Low", "first"],
        ["EQ_VegaCharge_Medium", "first"],
        ["EQ_VegaCharge_High", "first"],
        ["EQ_VegaCharge_MAX", "first"]
    ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["EQ_CurvatureCharge_Low", "first"],
        ["EQ_CurvatureCharge_Medium", "first"],
        ["EQ_CurvatureCharge_High", "first"],
        ["EQ_CurvatureCharge_MAX", "first"]
    ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "RatesEM"]]}],
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
        702.474388 + 29816.038563 + 0.]);
    let request = r#"
    {"measures": [
        ["EQ_TotalCharge_Low", "first"],
["EQ_TotalCharge_Medium", "first"],
["EQ_TotalCharge_High", "first"]
    ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
        "hide_zeros": true,
        "calc_params": {"jurisdiction": "BCBS"}
        
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn csr_nonsec_bcbs_delta() {
    let expected_res = arr1(&[
        45000.0, 975.0, 975.0, 684.920009, 768.283274, 843.4428, 656.018202, 742.954861, 820.733799, 820.733799
    ]);
    let request = r#"
    {"measures": [
        ["CSR_nonSec_DeltaSens", "sum"],
["CSR_nonSec_DeltaSens_Weighted", "sum"],
["CSR_nonSec_DeltaSb", "first"],
["CSR_nonSec_DeltaKb_Low", "first"],
["CSR_nonSec_DeltaKb_Medium", "first"],
["CSR_nonSec_DeltaKb_High", "first"],
            ["CSR_nonSec_DeltaCharge_Low", "first"],
["CSR_nonSec_DeltaCharge_Medium", "first"],
["CSR_nonSec_DeltaCharge_High", "first"],
["CSR_nonSec_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["CSR_nonSec_DeltaSens", "sum"],
["CSR_nonSec_DeltaSens_Weighted", "sum"],
["CSR_nonSec_DeltaSb", "first"],
["CSR_nonSec_DeltaKb_Low", "first"],
["CSR_nonSec_DeltaKb_Medium", "first"],
["CSR_nonSec_DeltaKb_High", "first"],
            ["CSR_nonSec_DeltaCharge_Low", "first"],
["CSR_nonSec_DeltaCharge_Medium", "first"],
["CSR_nonSec_DeltaCharge_High", "first"],
["CSR_nonSec_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
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
        ["CSR_nonSec_VegaSens", "sum"],
        ["CSR_nonSec_VegaSens_Weighted", "sum"],
        ["CSR_nonSec_VegaSb", "first"],
        ["CSR_nonSec_VegaKb_Low", "first"],
        ["CSR_nonSec_VegaKb_Medium", "first"],
        ["CSR_nonSec_VegaKb_High", "first"],
        ["CSR_nonSec_VegaCharge_Low", "first"],
        ["CSR_nonSec_VegaCharge_Medium", "first"],
        ["CSR_nonSec_VegaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "Rates"]]}],
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
        ["CSR_nonSec_VegaSens", "sum"],
        ["CSR_nonSec_VegaSens_Weighted", "sum"],
        ["CSR_nonSec_VegaSb", "first"],
        ["CSR_nonSec_VegaKb_Low", "first"],
        ["CSR_nonSec_VegaKb_Medium", "first"],
        ["CSR_nonSec_VegaKb_High", "first"],
        ["CSR_nonSec_VegaCharge_Low", "first"],
        ["CSR_nonSec_VegaCharge_Medium", "first"],
        ["CSR_nonSec_VegaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "Rates"]]}],
                "hide_zeros": true,
                "calc_params": {"jurisdiction": "CRR2"}
            
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn commodity_delta() {
    let expected_res = arr1(&[
        -250.0, -122.5, -122.5, 408.934179, 405.736564, 402.5, 269.704639, 260.4533, 250.861017,269.704639
    ]);
    let request = r#"
    {"measures": [
        ["Commodity_DeltaSens", "sum"],
        ["Commodity_DeltaSens_Weighted", "sum"],
        ["Commodity_DeltaSb", "first"],
        ["Commodity_DeltaKb_Low", "first"],
        ["Commodity_DeltaKb_Medium", "first"],
        ["Commodity_DeltaKb_High", "first"],
        ["Commodity_DeltaCharge_Low", "first"],
        ["Commodity_DeltaCharge_Medium", "first"],
        ["Commodity_DeltaCharge_High", "first"],
        ["Commodity_DeltaCharge_MAX", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
                "hide_zeros":true,
                "calc_params": {"jurisdiction": "BCBS"}
    }"#;
    assert_results(request, expected_res.sum(), None)
}

