//! Server side entry point
//! This to be conversted into server

use base_engine::prelude::*;
use std::process;
//use pretty_env_logger;
use log::info;
use serde::{Serialize, Deserialize};

fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();

    let conf = read_toml2::<DataSourceConfig>(SETUP).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    info!("Data SetUp: {:?}", conf);

    let data = conf.build_data();

    let message: Message = serde_json::from_str(JSON).unwrap();

    //recoverable. If not valid use default
    //let default_params = read_toml2::<FRTBRegParams>(REG_PARAMS)
    //.unwrap(); // recoverable, but for now ok to unwrap()

    match message {
        Message::Request{ params: conf, ..} => {
            match base_engine::execute(conf, &data){
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
/*
/// Sample request 2
const JSON2: &str = r#"
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

/// Sample request 2
const JSON3: &str = r#"
{"type": "Request",
    "id": "123", 
    "method": "None", 
    "params": {
        "measures": [["Delta", "sum"]],
        "groupby": ["Desk"],
        "reporting_ccy": "USD",
        "filters": [{"Neq":[ ["LegalEntity", "Asia"], ["Country", "UK"] ]}]
    }
}"#;
*/

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/examples/data/datasource_config.toml";
// Default regulatory parameters
//const REG_PARAMS: &str = r"frtb_engine/examples/data/reg_params.toml";
