use std::net::TcpListener;

use driver::api::run_server;  

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8000")
        .expect("Failed to bind random port");
    run_server(listener)?.await
    //.expect("Failed to bind address");
    //let _ = tokio::spawn(server);
}