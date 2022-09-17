// TODO find out hot to use just a sbm::LAZY_DASET

use base_engine::prelude::*;
use frtb_engine::prelude::*;
use ndarray::prelude::*;

use once_cell::sync::Lazy;
use polars::prelude::*;

pub static LAZY_DASET: Lazy<FRTBDataSet> = Lazy::new(|| {
    let conf_path = r"./tests/data/datasource_config.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path)
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    let mut data: FRTBDataSet = DataSet::build(conf);
    data.prepare();
    data
});

fn assert_results(req: &str, expected_sum: f64, epsilon: Option<f64>) {
    let ep = if let Some(e) = epsilon { e } else { 1e-5 };
    let data_req = serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let excl = data_req._groupby().clone();
    let res =
        base_engine::execute(data_req, &*LAZY_DASET).expect("Error while calculating results");
    dbg!(res.clone());
    let res_numeric = res
        .lazy()
        .select([col("*").exclude(excl)])
        .collect()
        .expect("Could not remove column");
    let res_arr = res_numeric
        .to_ndarray::<Float64Type>()
        .expect("Could not convert result to nd_array");
    // Slightly naive, but we assume if the sum is equivallent then the result is accurate
    dbg!(res_arr.sum());
    dbg!(expected_sum);
    assert!((res_arr.sum() - expected_sum).abs() < ep);
}

#[test]
fn drc_nonsec() {
    let expected_res = arr1(&[
        10000.0, 9527.777778, 1429.166667, 9527.777778, 0.0, 1429.166667, 0.0, 1.0,
        -10000.0, -5722.222222, 0.0, 0.0, -5722.222222, 0.0, 28.611111, 0.0,
        5000.0, 958.333333, 2396.548886, 10486.111111, -9527.777778, 2496.388889, 190.555556, 0.523942
    ]);

    let request = r#"
    {"measures": [
            ["DRC_NonSec_GrossJTD", "sum"],
            ["DRC_NonSec_GrossJTD_Scaled", "sum"],
            ["DRC_NonSec_CapitalCharge", "first"],
            ["DRC_NonSec_NetLongJTD", "first"],
            ["DRC_NonSec_NetShortJTD", "first"],
            ["DRC_NonSec_NetLongJTD_Weighted", "first"],
            ["DRC_NonSec_NetAbsShortJTD_Weighted", "first"],
            ["DRC_NonSec_HBR", "first"]
                ],
        "groupby": ["Desk", "BucketBCBS"],
        "filters": [],
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
        10000.0, 9527.777778, 1429.166667, 9527.777778, 0.0, 1429.166667, 0.0, 1.0,
        -10000.0, -5722.222222, 0.0, 0.0, -5722.222222, 0.0, 28.611111, 0.0,
        5000.0, 958.333333, 2385.595555, 10486.111111, -9527.777778, 2410.555556, 47.638889, 0.523942
    ]);

    let request = r#"
    {"measures": [
            ["DRC_NonSec_GrossJTD", "sum"],
            ["DRC_NonSec_GrossJTD_Scaled", "sum"],
            ["DRC_NonSec_CapitalCharge", "first"],
            ["DRC_NonSec_NetLongJTD", "first"],
            ["DRC_NonSec_NetShortJTD", "first"],
            ["DRC_NonSec_NetLongJTD_Weighted", "first"],
            ["DRC_NonSec_NetAbsShortJTD_Weighted", "first"],
            ["DRC_NonSec_HBR", "first"]
                ],
        "groupby": ["Desk", "BucketBCBS"],
        "filters": [],
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
fn overwrites() {
    let expected_res = arr1(&[3814.762221]);

    let request = r#"
    {"filters": [],

        "groupby": ["RiskClass", "Desk"],
        
        "overwrites": [{   "column": "SensWeights",
                          "value": "[0.005]",
                          "when": [{"Eq":[["RiskClass", "DRC_NonSec"]]},
                                    {"Eq":[["CreditQuality", "AA"]]}]
                    }],
        
        "measures": [
            ["DRC_NonSec_CapitalCharge", "first"]
                ],
        
        
        "hide_zeros": true,
        "calc_params": {
            "jurisdiction": "BCBS",
            "apply_fx_curv_div": "true",
            "drc_offset": "false"
        }}
"#;
    assert_results(request, expected_res.sum(), None)
}

/// Note: DRC Offsetting is not yet implemented 
#[test]
fn drc_secnonctp() {
    let expected_res = arr1(&[8998.888889]);

    let request = r#"
    {"filters": [],

        "groupby": ["RiskClass", "Desk"],
        
        "measures": [
            ["DRC_SecNonCTP_CapitalCharge", "first"]
                ],
        
        
        "hide_zeros": true,
        "calc_params": {
            "jurisdiction": "BCBS",
            "apply_fx_curv_div": "true"
        }}
"#;
    assert_results(request, expected_res.sum(), None)
}