//#![allow(unused_must_use)] // TODO - remove
//! tests/health_check.rs
//! `tokio::test` is the testing equivalent of `tokio::main`.
//! It also spares you from having to specify the `#[test]` attribute.
//!
//!
//! You can inspect what code gets generated using
//! `cargo expand --test health_check` (<- name of the test file)

use std::{net::TcpListener, sync::Arc};

use base_engine::DataSetBase;

#[tokio::test]
async fn health_check_works() {
    // No need to await here
    let addr = spawn_app();
    // We need to bring in `reqwest` to perform HTTP requests against our application.
    let client = reqwest::Client::new();
    // Act
    let response = client
        .get(&format!("{}/health_check", &addr))
        .send()
        .await
        .expect("Failed to execute request.");
    // Assert
    assert!(response.status().is_success());
    assert_eq!(Some(0), response.content_length());
}

// Launch our application in the background(via tokio::spawn)
fn spawn_app() -> String {
    //let listener = TcpListener::bind("127.0.0.1:0")
    //    .expect("Failed to bind random port");
    //// We retrieve the port assigned to us by the OS
    //let port = listener.local_addr().unwrap().port();
    //let ds = Arc::new(DataSetBase::default());
    //let server = driver::api::run_server(listener, ds).expect("Failed to bind address");
    //let _ = tokio::spawn(server);
    //// We return the application address to the caller!
    //format!("http://127.0.0.1:{}", port)
    format!("OK")
}