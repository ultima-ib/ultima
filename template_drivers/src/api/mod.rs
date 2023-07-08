//! This module builds App and is Server bin specific

use actix_web::{dev::Server, middleware::Logger, web::Data, App, HttpServer};

use std::{
    net::TcpListener,
    //path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
// use tokio::task;

use ultibi::{
    api::routers,
    //aggregations::BASE_CALCS, polars::prelude::PolarsError,
    AggregationRequest,
    DataSet,
};

// We don't use actix_web_static_files here since because we don't want to rebuild the whole
//project when working on the frontend
//use actix_web_static_files::ResourceFiles;
//include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub fn run_server(
    listener: TcpListener,
    ds: Arc<RwLock<dyn DataSet>>,
    _templates: Vec<AggregationRequest>,
) -> std::io::Result<Server> {
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let ds = Data::from(ds);
    let static_files_dir =
        std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let _templates = Data::new(_templates);

    let server = HttpServer::new(move || {
        //let auth = HttpAuthentication::basic(validator);

        // We don't use actix_web_static_files here since because we don't want to rebuild the whole
        // project when working on the frontend
        // let generated = generate();

        App::new()
            .wrap(Logger::default())
            //.wrap(auth)
            .configure(routers::configure())
            // must be the last one
            .service(actix_files::Files::new("/", &static_files_dir).index_file("index.html"))
            //.service(ResourceFiles::new("/", generated))
            .app_data(ds.clone())
            .app_data(_templates.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}
