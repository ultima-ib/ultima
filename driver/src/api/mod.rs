//! This module builds App and is Server bin specific

pub mod pagination;

use actix_web::{web::{self, Data}, App, HttpRequest, HttpServer, Responder, 
    HttpResponse, get, dev::Server, middleware::Logger, 
    //error::InternalError, http::StatusCode,
     Result};
use anyhow::Context;
use serde::Serialize;
use std::{net::TcpListener, sync::Arc};
use tokio::task;

use base_engine::{DataSet, AggregationRequest};
use base_engine::api::aggregations::BASE_CALCS;

// use uuid::Uuid;
// use tracing::Instrument; //enters the span we pass as argument
// every time self, the future, is polled; it exits the span every time the future is parked.

#[get("/health_check")]
async fn health_check(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}  

#[tracing::instrument( name = "Obtaining DataSet Info", skip(ds) )]
async fn dataset_info<DS: Serialize>(_: HttpRequest, ds: Data<DS>) -> impl Responder {  
    web::Json(ds)
}
/*
pub fn error_chain_fmt(
    e: &impl std::error::Error,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    writeln!(f, "{}\n", e)?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }
    Ok(())
}

impl std::fmt::Debug for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}

/// Client facing error
/// TODO return the above debug message to the client
#[derive(thiserror::Error)]
pub enum UserError {
    #[error("Computation failed")]
    ComputeError(#[source] anyhow::Error),
    #[error("Something went wrong")]
    UnexpectedError(#[from] anyhow::Error),
}

/// TODO we actually want to return more detailed error message to the client
#[tracing::instrument( name = "Request Execution", skip(data) )]
async fn execute(data: Data<Arc<dyn DataSet>>, req: web::Json<AggregationRequest>) 
-> Result<HttpResponse, InternalError<UserError>>  {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        base_engine::execute_aggregation(r, Arc::clone(data.get_ref()))
    }).await
    .context("Failed to spawn blocking task.")
    .map_err(|e|InternalError::new(UserError::UnexpectedError(e), StatusCode::INTERNAL_SERVER_ERROR))?;
    match  res {
        Ok(df) => Ok(HttpResponse::Ok().json(df)),
        Err(e) => {tracing::error!("Failed to execute query: {:?}", e); 
                                    Err(InternalError::new(UserError::ComputeError(e.into()), StatusCode::INTERNAL_SERVER_ERROR))}
    }
}
*/

#[tracing::instrument( name = "Request Execution", skip(data) )]
async fn execute(data: Data<Arc<dyn DataSet>>, req: web::Json<AggregationRequest>) 
-> Result<HttpResponse>  {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        base_engine::execute_aggregation(r, Arc::clone(data.get_ref()))
    }).await
    .context("Failed to spawn blocking task.")
    .map_err(|e|actix_web::error::ErrorInternalServerError(e))?;
    match  res {
        Ok(df) => Ok(HttpResponse::Ok().json(df)),
        Err(e) => {tracing::error!("Failed to execute query: {:?}", e); 
                                Err(actix_web::error::ErrorExpectationFailed(e))}
    }
}

async fn measures() -> impl Responder {
    let res = BASE_CALCS.iter().map(|(x, _)|*x).collect::<Vec<&str>>();
    web::Json(res)
}  

// TODO Why can't I use ds: impl DataSet ?
pub fn run_server(listener: TcpListener, ds: Arc<dyn DataSet>) -> std::io::Result<Server>
{
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let ds = Data::new(ds);

    let server = HttpServer::new( move|| {
        App::new()
        .wrap(Logger::default())
        .service(health_check)
        .service(
            web::scope("/FRTB")
            .route("", web::get().to(dataset_info::<Arc<dyn DataSet>>))
            .route("", web::post().to(execute))
        )
        .route("/aggtypes", web::get().to(measures))
        .app_data(ds.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}