//! Server side entry point
//! This to be conversted into server

mod measures;

use base_engine::prelude::*;
use std::process;
use std::sync::Arc;
use log::info;
use serde::{Serialize, Deserialize};

#[cfg(feature = "FRTB")]
use frtb_engine::prelude::*;

fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();

    let conf = read_toml2::<DataSourceConfig>(SETUP).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    info!("Data SetUp: {:?}", conf);

    let mut data = conf.build_data();

    #[cfg(feature = "FRTB")]
    if cfg!(feature = "FRTB") {
        data = data.prepare();
    }

    let numer_cols = data.numeric_columns_owned(vec![]);
    println!("numeric columns: {:?}", numer_cols);

    let (measure_col, measure_fn) 
    = measures::derive_bespoke_measures(numer_cols);

    let message: Message = serde_json::from_str(JSON).unwrap();

    let (arc_measure_col,
        arc_measure_fn) = 
        (Arc::new(measure_col), Arc::new(measure_fn));

    match message {
        Message::Request{ params: conf, ..} => {
            match base_engine::execute(conf, &data, Arc::clone(&arc_measure_col),
            Arc::clone(&arc_measure_fn)){
                Err(e) =>{ // eventually will be tokio::spawn_blocking
                    eprintln!("Application error: {:#?}", e);
                    process::exit(1);
                },
                Ok(df) => {
                    println!("result: {:?}", df)}
            }
        },
        _ => ()
    };
}


// public params: Request
// bespoke params: FRTBRequest
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    Request { id: String, method: String, params: DataRequestS },
    Response { id: String, result: PlaceHolder },
}

#[derive(Serialize, Deserialize)]
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
        "reporting_ccy": "USD",
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
        "measures": [["DeltaSpot", "sum"],["FXDeltaSens", "sum"]],
        "groupby": ["Desk"],
        "reporting_ccy": "USD",
        "filters": [{"Neq":[ ["LegalEntity", "Asia"], ["Country", "UK"] ]}]
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
        "reporting_ccy": "USD",
        "filters": []
    }
}"#;
//["DeltaWeights", "list"] , 
["TotalDeltaSens", "sum"],
["DeltaSpot", "sum"], 
["FXDeltaSens", "sum"], 
["FxDeltaSensWeighted", "sum"],
*/

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
        "reporting_ccy": "USD",
        "filters": []
    }
}"#;

//["GIRRDeltaChargeMedium", "first"],
//["GIRRDeltaChargeLow", "first"],
//["GIRRDeltaChargeHigh", "first"]

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/examples/data/datasource_config.toml";
// Default regulatory parameters
//const REG_PARAMS: &str = r"frtb_engine/examples/data/reg_params.toml";
