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

    let dataset = conf.build_data();
    println!("Delta: {:?}", dataset.delta.unwrap().collect().unwrap());

    let message: Message = serde_json::from_str(JSON).unwrap();

    //recoverable. If no
    let default_params = read_toml2::<FRTBRegParams>(REG_PARAMS); // recoverable

    match message {
        Message::Request{ params: conf, ..} => {
            match frtb_engine::sa_capital_old(conf){
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
        "filters": [{"On":["LegalEntity", "EU"]}]
    }
}"#;

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/examples/data/datasource_config.toml";
// Default regulatory parameters
const REG_PARAMS: &str = r"frtb_engine/examples/data/reg_params.toml";
