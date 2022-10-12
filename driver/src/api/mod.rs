//! This module builds App and is Server bin specific
#[cfg(feature = "FRTB")]
use frtb_engine::statics::{MEDIUM_CORR_SCENARIO};

pub mod pagination;

use actix_web::{
    dev::Server,
    get,
    middleware::Logger,
    web::{self, Data},
    App,
    HttpRequest,
    HttpResponse,
    HttpServer,
    Responder,
    //error::InternalError, http::StatusCode,
    Result,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{net::TcpListener, sync::Arc};
use tokio::task;

use base_engine::api::aggregations::BASE_CALCS;
use base_engine::{prelude::PolarsResult, AggregationRequest, DataSet};

// use uuid::Uuid;
// use tracing::Instrument; //enters the span we pass as argument
// every time self, the future, is polled; it exits the span every time the future is parked.

#[get("/scenarios/{scen}")]
async fn scenarios(path: web::Path<String>) -> Result<HttpResponse> {
    let scenario = path.into_inner();
    match &scenario as &str {
        #[cfg(feature = "FRTB")]
        "medium" => Ok(HttpResponse::Ok().json(&*MEDIUM_CORR_SCENARIO)),
        _ => Err(actix_web::error::ErrorBadRequest("Only medium scenario can be displayed currently"))
    }
}

#[get("/health_check")]
async fn health_check(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}

#[derive(Deserialize)]
struct Pagination {
    page: usize,
    pattern: String,
}
const PER_PAGE: u16 = 100;

// /{column_name}?page=2&per_page=30
#[get("/columns/{column_name}")]
async fn column_search(
    path: web::Path<String>,
    page: web::Query<Pagination>,
    data: Data<Arc<dyn DataSet>>,
) -> Result<HttpResponse> {
    let column_name = path.into_inner();
    let (page, pat) = (page.page, page.pattern.clone());
    let res = task::spawn_blocking(move || {
        let d = data.get_ref();
        let srs = d.frame().column(&column_name)?;
        let search = base_engine::searches::filter_contains_unique(srs, &pat)?;
        let first = page * PER_PAGE as usize;
        let last = first + PER_PAGE as usize;
        let s = search.slice(first as i64, last);
        PolarsResult::Ok(s)
    })
    .await
    .context("Failed to spawn blocking task.")
    .map_err(actix_web::error::ErrorInternalServerError)?;
    match res {
        Ok(srs) => Ok(HttpResponse::Ok().json(Vec::from(
            srs.utf8()
                .map_err(actix_web::error::ErrorInternalServerError)?,
        ))),
        Err(e) => {
            tracing::error!("Failed to execute query: {:?}", e);
            Err(actix_web::error::ErrorExpectationFailed(e))
        }
    }
}

#[tracing::instrument(name = "Obtaining DataSet Info", skip(ds))]
async fn dataset_info<DS: Serialize>(_: HttpRequest, ds: Data<DS>) -> impl Responder {
    web::Json(ds)
}
/* TODO this is not good enough, as we need to return a more detailed message to the client
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

#[tracing::instrument(name = "Request Execution", skip(data))]
async fn execute(
    data: Data<Arc<dyn DataSet>>,
    req: web::Json<AggregationRequest>,
) -> Result<HttpResponse> {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        base_engine::execute_aggregation(r, Arc::clone(data.get_ref()))
    })
    .await
    .context("Failed to spawn blocking task.")
    .map_err(actix_web::error::ErrorInternalServerError)?;
    match res {
        Ok(df) => Ok(HttpResponse::Ok().json(df)),
        Err(e) => {
            tracing::error!("Failed to execute query: {:?}", e);
            Err(actix_web::error::ErrorExpectationFailed(e))
        }
    }
}

async fn measures() -> impl Responder {
    let res = BASE_CALCS.iter().map(|(x, _)| *x).collect::<Vec<&str>>();
    web::Json(res)
}

#[get("/templates")]
async fn templates(_: HttpRequest, templates: Data<Vec<AggregationRequest>>) -> impl Responder {
    web::Json(templates)
}

// TODO Why can't I use ds: impl DataSet ?
pub fn run_server(listener: TcpListener, ds: Arc<dyn DataSet>, _templates: Vec<AggregationRequest>) -> std::io::Result<Server> {
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let ds = Data::new(ds);
    let _templates = Data::new(_templates);

    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .service(
                web::scope("/api")
                .service(health_check)
                .service(
                    web::scope("/FRTB")
                        .route("", web::get().to(dataset_info::<Arc<dyn DataSet>>))
                        .route("", web::post().to(execute))
                        .service(column_search)
                        .service(templates)
                        .service(scenarios),
                )
                .route("/aggtypes", web::get().to(measures))
            )
            .app_data(ds.clone())
            .app_data(_templates.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}
