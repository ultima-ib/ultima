//! Server side entry point
//! This to be conversted into server

use base_engine::prelude::*;

use std::fs;
//use std::sync::Arc;
#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;
use log::info;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

#[global_allocator]
#[cfg(not(target_os = "linux"))]
static ALLOC: MiMalloc = MiMalloc;

#[cfg(feature = "FRTB")]
type DataSetType = frtb_engine::FRTBDataSet<'static>;
#[cfg(not(feature = "FRTB"))]
type DataSetType = base_engine::DataSetBase<'static>;

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/tests/data/datasource_config.toml";

fn main() {
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();
    // Read Config
    let conf =
        read_toml2::<DataSourceConfig>(SETUP).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    info!("Data SetUp: {:?}", conf);

    // Build data
    let mut data = DataSetType::build(conf);
    //dbg!(&data);
    // Pre build some columns, which you wish to store in memory alongside the original data
    data.prepare();

    let json =
        fs::read_to_string("./driver/src/request.json").expect("Unable to read request file");

    let messages: Vec<Message> = serde_json::from_str(&json).unwrap();
    for message in messages{
        info!("{:?}", message);
        let now = Instant::now();
        if let Message::Request { params: conf, .. } = message {
            match base_engine::execute(conf, &data) {
                Err(e) => {
                    eprintln!("Application error: {:#?}", e);
                    continue;
                }
                Ok(df) => {
                    let elapsed = now.elapsed();
                    println!("result: {:?}", df);
                    println!("Elapsed: {:.6?}", elapsed);
                }
            }
        };
}
}

// public params: Request
// bespoke params: FRTBRequest
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    Request {
        id: String,
        method: String,
        params: DataRequestS,
    },
    Response {
        id: String,
        result: PlaceHolder,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct PlaceHolder(u8);

/*
/// Sample request
const JSON: &str = r#"
{"type": "Request",
    "id": "123",
    "method": "None",
    "params": {
        "cob": "2022-04-05",
        "measures": [["Delta", "sum"]],
        "groupby": ["Desk"],
        "filters": [{"Eq":[["LegalEntity", "EMEA"], ["Country", "UK"]]}]
    }
}"#;

/// Sample request 2
const JSON: &str = r#"
{"type": "Request",
    "id": "123",
    "method": "None",
    "params": {
        "cob": "2022-04-05",
        "measures": [["Delta", "sum"]],
        "groupby": ["Desk"],
        "reporting_ccy": "USD",
        "filters": [{"In":[["LegalEntity", ["EMEA"]], ["Country", ["UK", "China"]]]}]
    }
}"#;

/// Sample request 3
const JSON: &str = r#"
{"type": "Request",
    "id": "123",
    "method": "None",
    "params": {
        "measures": [
                    ["SensitivitySpot", "sum"],
                    ["FXDeltaSens", "sum"]
                    ["Commodity_DeltaCharge_Medium", "quantile95low"]
                    ],
        "groupby": ["Country", "Desk"],
        "filters": [
                    {"Neq":[ ["LegalEntity", "Asia"], ["Country", "UK"] ]},
                    {"In":[["LegalEntity", ["EMEA"]], ["Country", ["UK", "China"]]]}
                    ]
    }
}"#;


/// Sample request 4
const JSON: &str = r#"
{"type": "Request",
    "id": "123",
    "method": "None",
    "params": {
        "measures": [
            ["FXDeltaSens", "sum"],
            ["FxDeltaSensWeighted", "sum"],
            ["FxDeltaChargeLow", "first"],
            ["FxDeltaChargeMedium", "first"],
            ["FxDeltaChargeHigh", "first"]],
        "groupby": ["Desk"],
        "filters": []
    }
}"#;
["SensWeights", "list"] ,
["TotalDeltaSens", "sum"],
["SensitivitySpot", "sum"],
["FXDeltaSens", "sum"],
["FxDeltaSensWeighted", "sum"],


const JSON: &str = r#"
{"type": "Request",
    "id": "123",
    "method": "None",
    "params": {
        "measures": [
            ["GIRRDeltaChargeLow", "first"],
            ["GIRRDeltaChargeMedium", "first"],
            ["GIRRDeltaChargeHigh", "first"]
        ],
        "groupby": ["Desk"],
        "filters": []
    }
}"#;



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

["Commodity_VegaSens", "sum"],
["Commodity_VegaSens_Weighted", "sum"],
["Commodity_VegaSb", "first"],
["Commodity_VegaKb_Low", "first"],
["Commodity_VegaKb_Medium", "first"],
["Commodity_VegaKb_High", "first"],
["Commodity_VegaCharge_Low", "first"],
["Commodity_VegaCharge_Medium", "first"],
["Commodity_VegaCharge_High", "first"],
["Commodity_VegaCharge_MAX", "first"],

["Commodity_CurvatureDelta", "sum"],
["Commodity_CurvatureDelta_Weighted", "sum"],
["Commodity_PnLup", "sum"],
["Commodity_PnLdown", "sum"],
["Commodity_CVRup", "sum"],
["Commodity_CVRdown", "sum"],
["Commodity_Curvature_KbPlus_Low", "first"],
["Commodity_Curvature_KbMinus_Low", "first"],
["Commodity_Curvature_Kb_Low", "first"],
["Commodity_Curvature_Sb_Low", "first"],
["Commodity_CurvatureCharge_Low", "first"],
["Commodity_Curvature_KbPlus_Medium", "first"],
["Commodity_Curvature_KbMinus_Medium", "first"],
["Commodity_Curvature_Kb_Medium", "first"],
["Commodity_Curvature_Sb_Medium", "first"],
["Commodity_CurvatureCharge_Medium", "first"],
["Commodity_Curvature_KbPlus_High", "first"],
["Commodity_Curvature_KbMinus_High", "first"],
["Commodity_Curvature_Kb_High", "first"],
["Commodity_Curvature_Sb_High", "first"],
["Commodity_CurvatureCharge_MAX", "first"],

["Commodity_TotalCharge_Low", "first"],
["Commodity_TotalCharge_Medium", "first"],
["Commodity_TotalCharge_High", "first"],
["Commodity_TotalCharge_MAX", "first"],



["CSR_nonSec_DeltaSens", "sum"],
["CSR_nonSec_DeltaSens_Weighted", "sum"],
["CSR_nonSec_DeltaSb", "first"],
["CSR_nonSec_DeltaKb_Low", "first"],
["CSR_nonSec_DeltaKb_Medium", "first"],
["CSR_nonSec_DeltaKb_High", "first"],
["CSR_nonSec_DeltaCharge_Low", "first"],
["CSR_nonSec_DeltaCharge_Medium", "first"],
["CSR_nonSec_DeltaCharge_High", "first"],
["CSR_nonSec_DeltaCharge_MAX", "first"],

["CSR_nonSec_VegaSens", "sum"],
["CSR_nonSec_VegaSens_Weighted", "sum"],
["CSR_nonSec_VegaSb", "first"],
["CSR_nonSec_VegaKb_Low", "first"],
["CSR_nonSec_VegaKb_Medium", "first"],
["CSR_nonSec_VegaKb_High", "first"],
["CSR_nonSec_VegaCharge_Low", "first"],
["CSR_nonSec_VegaCharge_Medium", "first"],
["CSR_nonSec_VegaCharge_High", "first"],
["CSR_nonSec_VegaCharge_MAX", "first"],

["CSR_nonSec_CurvatureDelta", "sum"],
["CSR_nonSec_CurvatureDelta_Weighted", "sum"],
["CSR_nonSec_PnLup", "sum"],
["CSR_nonSec_PnLdown", "sum"],
["CSR_nonSec_CVRup", "sum"],
["CSR_nonSec_CVRdown", "sum"],
["CSR_nonSec_Curvature_KbPlus_Low", "first"],
["CSR_nonSec_Curvature_KbMinus_Low", "first"],
["CSR_nonSec_Curvature_Kb_Low", "first"],
["CSR_nonSec_Curvature_Sb_Low", "first"],
["CSR_nonSec_CurvatureCharge_Low", "first"],
["CSR_nonSec_Curvature_KbPlus_Medium", "first"],
["CSR_nonSec_Curvature_KbMinus_Medium", "first"],
["CSR_nonSec_Curvature_Kb_Medium", "first"],
["CSR_nonSec_Curvature_Sb_Medium", "first"],
["CSR_nonSec_CurvatureCharge_Medium", "first"],
["CSR_nonSec_Curvature_KbPlus_High", "first"],
["CSR_nonSec_Curvature_KbMinus_High", "first"],
["CSR_nonSec_Curvature_Kb_High", "first"],
["CSR_nonSec_Curvature_Sb_High", "first"]
["CSR_nonSec_CurvatureCharge_High", "first"],
["CSR_nonSec_CurvatureCharge_MAX", "first"],

["CSR_nonSec_TotalCharge_Low", "first"],
["CSR_nonSec_TotalCharge_Medium", "first"],
["CSR_nonSec_TotalCharge_High", "first"],



["CSR_secCTP_DeltaSens", "sum"],
["CSR_secCTP_DeltaSens_Weighted", "sum"],
["CSR_secCTP_DeltaSb", "first"],
["CSR_secCTP_DeltaKb_Low", "first"],
["CSR_secCTP_DeltaKb_Medium", "first"],
["CSR_secCTP_DeltaKb_High", "first"],
["CSR_secCTP_DeltaCharge_Low", "first"],
["CSR_secCTP_DeltaCharge_Medium", "first"],
["CSR_secCTP_DeltaCharge_High", "first"],

["CSR_secCTP_VegaSens", "sum"],
["CSR_secCTP_VegaSens_Weighted", "sum"],
["CSR_secCTP_VegaSb", "first"],
["CSR_secCTP_VegaKb_Low", "first"],
["CSR_secCTP_VegaKb_Medium", "first"],
["CSR_secCTP_VegaCharge_High", "first"],
["CSR_secCTP_VegaCharge_Low", "first"],
["CSR_secCTP_VegaCharge_Medium", "first"],
["CSR_secCTP_VegaKb_High", "first"],

["CSR_secCTP_CurvatureDelta", "sum"],
["CSR_secCTP_CurvatureDelta_Weighted", "sum"],
["CSR_secCTP_PnLup", "sum"],
["CSR_secCTP_PnLdown", "sum"],
["CSR_secCTP_CVRup", "sum"],
["CSR_secCTP_CVRdown", "sum"],
["CSR_secCTP_Curvature_KbPlus_Low", "first"],
["CSR_secCTP_Curvature_KbMinus_Low", "first"],
["CSR_secCTP_Curvature_Kb_Low", "first"],
["CSR_secCTP_Curvature_Sb_Low", "first"],
["CSR_secCTP_CurvatureCharge_Low", "first"],
["CSR_secCTP_Curvature_KbPlus_Medium", "first"],
["CSR_secCTP_Curvature_KbMinus_Medium", "first"],
["CSR_secCTP_Curvature_Kb_Medium", "first"],
["CSR_secCTP_Curvature_Sb_Medium", "first"],
["CSR_secCTP_CurvatureCharge_Medium", "first"],
["CSR_secCTP_Curvature_KbPlus_High", "first"],
["CSR_secCTP_Curvature_KbMinus_High", "first"],
["CSR_secCTP_Curvature_Kb_High", "first"],
["CSR_secCTP_Curvature_Sb_High", "first"]




["CSR_Sec_nonCTP_DeltaSens", "sum"],
["CSR_Sec_nonCTP_DeltaSens_Weighted", "sum"],
["CSR_Sec_nonCTP_DeltaSb", "first"],
["CSR_Sec_nonCTP_DeltaKb_Low", "first"],
["CSR_Sec_nonCTP_DeltaKb_Medium", "first"],
["CSR_Sec_nonCTP_DeltaKb_High", "first"],
["CSR_Sec_nonCTP_DeltaCharge_Low", "first"],
["CSR_Sec_nonCTP_DeltaCharge_Medium", "first"],
["CSR_Sec_nonCTP_DeltaCharge_High", "first"],

["CSR_Sec_nonCTP_VegaSens", "sum"],
["CSR_Sec_nonCTP_VegaSens_Weighted", "sum"],
["CSR_Sec_nonCTP_VegaSb", "first"],
["CSR_Sec_nonCTP_VegaKb_Low", "first"],
["CSR_Sec_nonCTP_VegaKb_Medium", "first"],
["CSR_Sec_nonCTP_VegaCharge_High", "first"],
["CSR_Sec_nonCTP_VegaCharge_Low", "first"],
["CSR_Sec_nonCTP_VegaCharge_Medium", "first"],
["CSR_Sec_nonCTP_VegaKb_High", "first"],

["CSR_Sec_nonCTP_CurvatureDelta", "sum"],
["CSR_Sec_nonCTP_CurvatureDelta_Weighted", "sum"],
["CSR_Sec_nonCTP_PnLup", "sum"],
["CSR_Sec_nonCTP_PnLdown", "sum"],
["CSR_Sec_nonCTP_CVRup", "sum"],
["CSR_Sec_nonCTP_CVRdown", "sum"],
["CSR_Sec_nonCTP_Curvature_KbPlus_Low", "first"],
["CSR_Sec_nonCTP_Curvature_KbMinus_Low", "first"],
["CSR_Sec_nonCTP_Curvature_Kb_Low", "first"],
["CSR_Sec_nonCTP_Curvature_Sb_Low", "first"],
["CSR_Sec_nonCTP_Curvature_KbPlus_Medium", "first"],
["CSR_Sec_nonCTP_Curvature_KbMinus_Medium", "first"],
["CSR_Sec_nonCTP_Curvature_Kb_Medium", "first"],
["CSR_Sec_nonCTP_Curvature_Sb_Medium", "first"],
["CSR_Sec_nonCTP_Curvature_KbPlus_High", "first"],
["CSR_Sec_nonCTP_Curvature_KbMinus_High", "first"],
["CSR_Sec_nonCTP_Curvature_Kb_High", "first"],
["CSR_Sec_nonCTP_Curvature_Sb_High", "first"],
["CSR_Sec_nonCTP_CurvatureCharge_Low", "first"],
["CSR_Sec_nonCTP_CurvatureCharge_Medium", "first"],
["CSR_Sec_nonCTP_CurvatureCharge_High", "first"]


["EQ_DeltaSens", "sum"],
["EQ_DeltaSens_Weighted", "sum"],
["EQ_DeltaSb", "first"],
["EQ_DeltaKb_Low", "first"],
["EQ_DeltaKb_Medium", "first"],
["EQ_DeltaKb_High", "first"],
["EQ_DeltaCharge_Low", "first"],
["EQ_DeltaCharge_Medium", "first"],
["EQ_DeltaCharge_High", "first"],
["EQ_DeltaCharge_MAX", "first"],

["EQ_VegaSens", "sum"],
["EQ_VegaSens_Weighted", "sum"],
["EQ_VegaSb", "first"],
["EQ_VegaKb_Low", "first"],
["EQ_VegaKb_Medium", "first"],
["EQ_VegaKb_High", "first"],
["EQ_VegaCharge_Low", "first"],
["EQ_VegaCharge_Medium", "first"],
["EQ_VegaCharge_High", "first"],
["EQ_VegaCharge_MAX", "first"],

["EQ_CurvatureDelta", "sum"],
["EQ_CurvatureDelta_Weighted", "sum"],
["EQ_PnLup", "sum"],
["EQ_PnLdown", "sum"],
["EQ_CVRup", "sum"],
["EQ_CVRdown", "sum"],
["EQ_Curvature_KbPlus_Low", "first"],
["EQ_Curvature_KbMinus_Low", "first"],
["EQ_Curvature_Kb_Low", "first"],
["EQ_Curvature_Sb_Low", "first"],
["EQ_Curvature_KbPlus_Medium", "first"],
["EQ_Curvature_KbMinus_Medium", "first"],
["EQ_Curvature_Kb_Medium", "first"],
["EQ_Curvature_Sb_Medium", "first"],
["EQ_Curvature_KbPlus_High", "first"],
["EQ_Curvature_KbMinus_High", "first"],
["EQ_Curvature_Kb_High", "first"],
["EQ_Curvature_Sb_High", "first"],
["EQ_CurvatureCharge_Low", "first"],
["EQ_CurvatureCharge_Medium", "first"],
["EQ_CurvatureCharge_High", "first"]
["EQ_CurvatureCharge_MAX", "first"],

["EQ_TotalCharge_Low", "first"],
["EQ_TotalCharge_Medium", "first"],
["EQ_TotalCharge_High", "first"],



["FX_DeltaSens", "sum"],
["FX_DeltaSens_Weighted", "sum"],
["FX_DeltaSb", "first"],
["FX_DeltaKb", "first"],
["FX_DeltaCharge_Low", "first"],
["FX_DeltaCharge_Medium", "first"],
["FX_DeltaCharge_High", "first"],
["FX_DeltaCharge_MAX", "first"],

["FX_VegaSens", "sum"],
["FX_VegaSens_Weighted", "sum"],
["FX_VegaSb", "first"],
["FX_VegaKb_Low", "first"],
["FX_VegaKb_Medium", "first"],
["FX_VegaKb_High", "first"],
["FX_VegaCharge_Low", "first"],
["FX_VegaCharge_Medium", "first"],
["FX_VegaCharge_High", "first"],
["FX_VegaCharge_MAX", "first"],

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
["FX_CurvatureCharge_MAX", "first"],

["FX_TotalCharge_Low", "first"],
["FX_TotalCharge_Medium", "first"],
["FX_TotalCharge_High", "first"],



["GIRR_DeltaSens", "sum"],
["GIRR_DeltaSens_Weighted", "sum"],
["GIRR_DeltaSb", "first"],
["GIRR_DeltaKb_Low", "first"],
["GIRR_DeltaKb_Medium", "first"],
["GIRR_DeltaKb_High", "first"],
["GIRR_DeltaCharge_Low", "first"],
["GIRR_DeltaCharge_Medium", "first"],
["GIRR_DeltaCharge_High", "first"],
["GIRR_DeltaCharge_MAX", "first"],

["GIRR_VegaSens", "sum"],
["GIRR_VegaSens_Weighted", "sum"],
["GIRR_VegaSb", "first"],
["GIRR_VegaKb_Low", "first"],
["GIRR_VegaKb_Medium", "first"],
["GIRR_VegaKb_High", "first"],
["GIRR_VegaCharge_Low", "first"],
["GIRR_VegaCharge_Medium", "first"],
["GIRR_VegaCharge_High", "first"],
["GIRR_VegaCharge_MAX", "first"],

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
["GIRR_CurvatureCharge_MAX", "first"],

["GIRR_TotalCharge_Low", "first"],
["GIRR_TotalCharge_Medium", "first"],
["GIRR_TotalCharge_High", "first"],


["PnL_Up", "sum"],
["PnL_Down", "sum"]

"reporting_ccy": "USD"
"filters": [{"Eq":[["Desk", "FXOptions"]]}],
"base_csr_nonsec_tenor_rho": "{\"v\":1,\"dim\":[5,5],\"data\":[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]}"
"base_csr_nonsec_diff_name_rho_per_bucket": "[1.0,2.0]" <- Example of bad input. Parsing would go for a default
["Desk","FXCash"],["Desk","RatesEM"]
*/
