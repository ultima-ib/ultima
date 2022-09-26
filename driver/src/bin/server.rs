use std::{net::TcpListener, sync::Arc};

use driver::api::run_server; 
use driver::helpers::acquire;
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

//to be passed as a command line argument
const SETUP: &str = r"frtb_engine/tests/data/datasource_config.toml";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    //let data: Arc<dyn DataSet> = Arc::new(acquire::data::<DataSetType>(SETUP));
    //let data = Arc::new(acquire::data::<DataSetType>(SETUP));
    let data = acquire::data::<DataSetType>(SETUP);

    let listener = TcpListener::bind("127.0.0.1:8000")
        .expect("Failed to bind random port");
    run_server(listener, Arc::new(data))?.await
}