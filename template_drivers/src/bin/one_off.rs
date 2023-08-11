//! Server side entry point
//! This to be conversted into server

use clap::Parser;
use ultibi::AggregationRequest;
//use base_engine::prelude::*;
use template_drivers::helpers::{acquire, cli::CliOnce};

use log::{error, info};
use std::time::Instant;
use std::{fs, sync::Arc};

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;
#[global_allocator]
#[cfg(not(target_os = "linux"))]
static ALLOC: MiMalloc = MiMalloc;

// TODO is there a way to get rid of type definition here? In order to allow user to
// use bin without any need to modify the script? May be via features?
#[cfg(feature = "FRTB")]
pub type DataSetType = frtb_engine::FRTBDataSet;
#[cfg(not(feature = "FRTB"))]
pub type DataSetType = ultibi::DataSetBase;

#[allow(clippy::uninlined_format_args)]
fn main() -> anyhow::Result<()> {
    // Read .env
    // TODO in production use env variables, not .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let cli = CliOnce::parse();
    let setup_path = cli.config;
    let requests_path = cli.requests;

    // Build Data
    let data = acquire::data::<DataSetType>(setup_path.as_str());

    let arc_data = Arc::new(data);

    let json = fs::read_to_string(requests_path.as_str()).expect("Unable to read request file");

    // Later this will be RequestE (to match other requests as well)
    let requests: Vec<AggregationRequest> = serde_json::from_str(&json).expect("Bad requests");

    // From here we do not panic
    for request in requests {
        //let rqst_str = serde_json::to_string(&request);
        info!("{:?}", request);
        let now = Instant::now();
        match ultibi::exec_agg(
            &*Arc::clone(&arc_data),
            request,
            cfg!(feature = "streaming"),
        ) {
            Err(e) => {
                error!("Application error: {:#?}", e);
                continue;
            }

            Ok(df) => {
                let elapsed = now.elapsed();
                println!("result: {df}");
                println!("Time to Compute: {:.6?}", elapsed);
            }
        }
    }
    Ok(())
}
