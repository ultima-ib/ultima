use std::net::SocketAddr;
use std::{env, fs};
use std::{net::TcpListener, sync::Arc};

use base_engine::AggregationRequest;
use clap::Parser;
use driver::api::run_server;
use driver::helpers::{acquire, cli::CliServer, cli::REQUESTS};
//use log::info;

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

#[cfg(feature = "FRTB")]
pub type DataSetType = frtb_engine::FRTBDataSet;
#[cfg(not(feature = "FRTB"))]
pub type DataSetType = base_engine::DataSetBase;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let cli = CliServer::parse();
    let setup_path = cli.config;
    let requests_path = cli.requests;

    let _requests: Vec<AggregationRequest> = if cli.host {
        // TODO remove this part. When going live, for cli host 
        // ie this block will be replaced to return vec![] if requests_path is None.
        let json = fs::read_to_string(
            requests_path
            .unwrap_or(REQUESTS.into())
            .as_str(),
        )
        .expect("Couldn't read requests path");
        serde_json::from_str(&json).expect("Couldn't parse requests file")
    } else {
        let json = fs::read_to_string(
            requests_path
                .expect("Please provide requests path")
                .as_str(),
        )
        .expect("Couldn't read requests path");
        serde_json::from_str(&json).expect("Couldn't parse requests file")
    };

    let addr: SocketAddr = cli
        .address // command line arg first
        .or_else(|| env::var("ADDRESS").ok()) // OR use .env
        .and_then(|addr| addr.parse().ok())
        .or_else(|| Some(([127, 0, 0, 1], 8080).into())) // Finaly, this default
        .expect("can't parse ADDRES variable");

    let data = acquire::data::<DataSetType>(setup_path.as_str());

    let listener = TcpListener::bind(addr).expect("Failed to bind random port");

    run_server(listener, Arc::new(data), _requests)?.await
}
