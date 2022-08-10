use base_engine::prelude::*;
use frtb_engine::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;
use once_cell::sync::Lazy;

pub static LAZY_DASET: Lazy<FRTBDataSet>  = Lazy::new(|| {
    let conf_path =  r"./tests/data/datasource_config.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    let mut data: FRTBDataSet = DataSet::build(conf);
    data.prepare();
    data
});

fn assert_results(req: &str, expected_sum: f64, epsilon: Option<f64>) {
    let ep = if let Some(e) = epsilon {e} else{1e-5};
    let data_req = serde_json::from_str::<DataRequestS>(req).expect("Could not parse request");
    let excl = data_req._groupby().clone();
    let res = base_engine::execute(data_req, &*LAZY_DASET).expect("Error while calculating results");
    dbg!(res.clone());
    let res_numeric = res.lazy().select([col("*").exclude(excl)]).collect().expect("Could not remove column");
    let res_arr = res_numeric.to_ndarray::<Float64Type>().expect("Could not convert result to nd_array");
    // Slightly naive, but we assume if the sum is equivallent then the result is accurate
    dbg!(res_arr.sum());
    dbg!(expected_sum);
    assert!((res_arr.sum()-expected_sum)<ep);
}

#[test]
fn fx_delta() {
    let expected_res = arr1(&[115.0, 12.197592, 12.197592, 12.197592, 11.652789, 11.803866, 11.953033]);
    let request = r#"
    {"measures": [
        ["FX_DeltaSens", "sum"],
        ["FX_DeltaSens_Weighted", "sum"],
        ["FX_DeltaSb", "first"],
        ["FX_DeltaKb", "first"],
        ["FX_DeltaCharge_Low", "first"],
        ["FX_DeltaCharge_Medium", "first"],
        ["FX_DeltaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    "optional_params": {
                "calc_params": {"jurisdiction": "BCBS"}
            }
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn fx_vega() {
    let expected_res = arr1(&[53000.0, 53000.0, 53000.0, 50894.787649, 51958.261414, 53000.0, 49423.256786, 50875.624376,52287.6658]);
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
        ["FX_VegaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    "optional_params": {
                "calc_params": {"jurisdiction": "BCBS"}
            }
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn fx_curvature() {
    let expected_res = arr1(&[ 369000.0, 39138.360339, 3000.0, -3000.0, 28107.613597, -28107.613597, 28107.613597, 0.0, 28107.613597, 28107.613597, 23550.772425, 24159.070424, 24752.423835]);
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
["FX_CurvatureCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "RatesEM"]]}],
    "optional_params": {
        "hide_zeros": true,
                "calc_params": {"jurisdiction": "BCBS",
                "apply_fx_curv_div": "true"}
            }
    }"#;
    assert_results(request, dbg!(expected_res).sum(), None)
}

#[test]
fn girr_delta() {
    let expected_res = arr1(&[2581.0, 35.925432, 2155.525895, 28.072696, 29.023816, 29.941875, 26.770639, 28.0824, 29.335659]);
    let request = r#"
    {"measures": [
        ["GIRR_DeltaSens", "sum"],
["GIRR_DeltaSens_Weighted", "sum"],
["GIRR_DeltaSb", "sum"],
["GIRR_DeltaKb_Low", "first"],
["GIRR_DeltaKb_Medium", "first"],
["GIRR_DeltaKb_High", "first"],
["GIRR_DeltaCharge_Low", "first"],
["GIRR_DeltaCharge_Medium", "first"],
["GIRR_DeltaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    "optional_params": {
                "calc_params": {"jurisdiction": "BCBS"}
            }
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn girr_vega() {
    let expected_res = arr1(&[210000.0, 210000.0, 210000.0, 157611.879405, 163407.920578, 169005.3031, 143838.458921, 156128.390288, 167519.092174]);
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
        ["GIRR_VegaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [{"Eq":[["Desk", "FXOptions"]]}],
    "optional_params": {
                "calc_params": {"jurisdiction": "BCBS"}
            }
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn girr_curvature() {
    let expected_res = arr2(&[
        [270000.0, 22000.0, -15000.0, 3992.497834, -18007.502166, 11007.502166, 0.0, 11007.502166, 11007.502166, 11007.502166, 8567.192327, 8779.024461, 8985.864266],
        [270000.0, 22000.0, -15000.0, 3992.497834, -18007.502166, 11007.502166, 0.0, 11007.502166, 11007.502166, 11007.502166, 8567.192327, 8779.024461, 8985.864266]]);
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
["GIRR_CurvatureCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "optional_params": {
                "hide_zeros":true,
                "calc_params": {"jurisdiction": "BCBS"}
            }
    }"#;
    assert_results(request, expected_res.sum(), Some(1e-4))
}

#[test]
fn eq_delta() {
    let expected_res = arr1(&[
        2800.0, 1089.0, 1089.0, 987.273493, 995.758659, 1004.116547, 665.011398, 683.999424, 702.474388]);
    let request = r#"
    {"measures": [
        ["Equity_DeltaSens", "sum"],
["Equity_DeltaSens_Weighted", "sum"],
["Equity_DeltaSb", "first"],
["Equity_DeltaKb_Low", "first"],
["Equity_DeltaKb_Medium", "first"],
["Equity_DeltaKb_High", "first"],
["Equity_DeltaCharge_Low", "first"],
["Equity_DeltaCharge_Medium", "first"],
["Equity_DeltaCharge_High", "first"]
            ],
    "groupby": ["Desk"],
    "filters": [],
    "optional_params": {
                "hide_zeros":true,
                "calc_params": {"jurisdiction": "CRR2"}
            }
    }"#;
    assert_results(request, expected_res.sum(), None)
}


