//! Server side entry point
//! This to be conversted into server

use frtb_engine::prelude::*;
use std::{process, str::FromStr};
//use pretty_env_logger;
use log::{debug, info, trace, warn};
use serde::{Serialize, Deserialize};
use serde_json::value::Serializer;
use chrono::NaiveDate;

fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();

    let conf = read_toml2::<FRTBDataSetUp>(SETUP).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    info!("Data SetUp: {:?}", conf);

    let data = conf.build_data();

    //let a = FRTBFilter::On(vec![("Anatoly".to_string(), "Bugakov".to_string())]);
    //let z = serde_json::to_string::<FRTBFilter>(&a).unwrap();
    //debug!("FRTBFilter: {:?}", z);

    let message: Message = serde_json::from_str(JSON).unwrap();

    //recoverable. If not valid use default
    let default_params = read_toml2::<FRTBRegParams>(REG_PARAMS)
    .unwrap(); // recoverable, but for now ok to unwrap()

    // Example setup to validate possible filter/groupby
    let groups: Vec<String> = vec!["TradeId".to_string(), "RiskClass".to_string(), "RiskFactor".to_string()];
    let groups = data.derive_groups(groups);
    println!("Groups: {:?}", groups);

    match message {
        Message::Request{ params: conf, ..} => {
            match frtb_engine::sa_capital(conf, data, default_params){
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


#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    Request { id: String, method: String, params: FRTBRequest },
    Response { id: String, result: PlaceHolder },
}

#[derive(Serialize, Deserialize)]
struct PlaceHolder(u8);

/// Sample request
const JSON: &str = r#"
{"type": "Request",
    "id": "123", 
    "method": "None", 
    "params": {
        "cob": "2022-04-05",
        "measures": ["FX_Delta"],
        "groupby": ["Desk"],
        "reporting_ccy": "USD",
        "filters": [{"On":[["LegalEntity", "EU"], ["Country", "UK"]]}]
    }
}"#;

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/examples/data/datasource_config.toml";
// Default regulatory parameters
const REG_PARAMS: &str = r"frtb_engine/examples/data/reg_params.toml";
