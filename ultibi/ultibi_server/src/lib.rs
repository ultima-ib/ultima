#![doc(html_no_source)]

use std::sync::RwLock;
use std::{net::SocketAddr, sync::Arc};

use std::env;
use std::net::TcpListener;
use tokio::runtime::Builder;
use ultibi_core::DataSet;

use log::info;

pub mod api;
mod app;
mod helpers;
mod visual;

pub use visual::VisualDataSet;

#[allow(unused_must_use)]
fn run_server(ds: Arc<RwLock<dyn DataSet>>) {
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    //let listener = TcpListener::bind("127.0.0.1:0")
    //    .expect("Failed to bind random port");

    match env::var("RUST_LOG") {
        Ok(_) => (),
        Err(_) => {
            env::set_var("RUST_LOG", "info");
        }
    }

    let addr: SocketAddr = env::var("ADDRESS")
        .ok() // OR use .env
        .and_then(|addr| addr.parse().ok())
        .or_else(|| Some(([127, 0, 0, 1], 8080).into())) // Finaly, this default
        .expect("can't parse ADDRES variable");

    let listener = TcpListener::bind(addr).expect("Failed to bind random port");

    // We retrieve the port assigned to us by the OS
    let port = listener.local_addr().unwrap().port();
    info!("http://localhost:{port}");
    let url = format!("http://localhost:{port}");
    dbg!(url);

    runtime.block_on(crate::app::build_app(listener, ds, vec![]).expect("Failed to bind address"));
}
