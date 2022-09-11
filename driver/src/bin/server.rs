use driver::api::run_server;  

#[tokio::main]
async fn main() -> std::io::Result<()> {
    run_server("127.0.0.1:8000")?.await
    //.expect("Failed to bind address");
    //let _ = tokio::spawn(server);
}