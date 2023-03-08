use std::{sync::Arc, net::SocketAddr};

use tokio::{runtime::Builder};
use ultibi_core::DataSet;
use std::net::TcpListener;
use std::env;

use log::info;

mod app;
mod visual;

pub use visual::VisualDataSet;

#[allow(unused_must_use)]
fn run_server<DS: DataSet + 'static>(ds: Arc<DS>) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    //let listener = TcpListener::bind("127.0.0.1:0")
    //    .expect("Failed to bind random port");

    let addr: SocketAddr = env::var("ADDRESS").ok() // OR use .env
        .and_then(|addr| addr.parse().ok())
        .or_else(|| Some(([127, 0, 0, 1], 8080).into())) // Finaly, this default
        .expect("can't parse ADDRES variable");

    let listener = TcpListener::bind(addr).expect("Failed to bind random port");
    
    // We retrieve the port assigned to us by the OS
    let port = listener.local_addr().unwrap().port();
    info!("http://127.0.0.1:{port}");
    let url = format!("http://127.0.0.1:{port}");
    dbg!(url);

    runtime.block_on(
        crate::app::build_app(listener, ds, vec![])
        .expect("Failed to bind address")
    );
}