use std::net::SocketAddr;
use std::sync::RwLock;
use std::{env, fs};
use std::{net::TcpListener, sync::Arc};

use clap::Parser;
use template_drivers::api::run_server;
use template_drivers::helpers::{acquire, cli::CliServer};
use ultibi::AggregationRequest;
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
pub type DataSetType = ultibi::DataSetBase;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let cli = CliServer::parse();
    let setup_path = cli.config;
    let requests_path = cli.requests;

    let _requests: Vec<AggregationRequest> = requests_path
        .map(|path| fs::read_to_string(path.as_str()).expect("Couldn't read requests path"))
        .map(|file_as_str| {
            serde_json::from_str::<Vec<AggregationRequest>>(&file_as_str)
                .expect("Couldn't parse requests file")
        })
        .unwrap_or_default();

    let addr: SocketAddr = cli
        .address // command line arg first
        .or_else(|| env::var("ADDRESS").ok()) // OR use .env
        .and_then(|addr| addr.parse().ok())
        .or_else(|| Some(([127, 0, 0, 1], 8080).into())) // Finaly, this default
        .expect("can't parse ADDRES variable");

    let listener = TcpListener::bind(addr).expect("Failed to bind random port");
    let data = acquire::data::<DataSetType>(setup_path.as_str(), cfg!(feature = "streaming"));
    run_server(listener, Arc::new(RwLock::new(data)), _requests)?.await
}
