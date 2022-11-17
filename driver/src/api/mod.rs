//! This module builds App and is Server bin specific
#[cfg(feature = "FRTB")]
use frtb_engine::statics::MEDIUM_CORR_SCENARIO;

pub mod pagination;

use actix_files as fs;
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
//use dashmap::DashMap;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{net::TcpListener, sync::Arc};
use tokio::task;

use base_engine::{
    api::aggregations::BASE_CALCS, col, prelude::PolarsResult, AggregationRequest, DataSet,
};

#[cfg(feature = "cache")]
pub type CACHE = base_engine::execution_with_cache::CACHE;
#[cfg(not(feature = "cache"))]
pub type CACHE = std::collections::HashMap<String, String>; // dummy, not used if cache feature is not activated

// use uuid::Uuid;
// use tracing::Instrument; //enters the span we pass as argument
// every time self, the future, is polled; it exits the span every time the future is parked.

#[get("/scenarios/{scen}")]
async fn scenarios(path: web::Path<String>) -> Result<HttpResponse> {
    let scenario = path.into_inner();
    match &scenario as &str {
        #[cfg(feature = "FRTB")]
        "medium" => Ok(HttpResponse::Ok().json(&*MEDIUM_CORR_SCENARIO)),
        _ => Err(actix_web::error::ErrorBadRequest(
            "Only medium scenario can be displayed currently",
        )),
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
        let lf = d.lazy_frame();
        let df = lf.clone().select([col(&column_name)]).collect()?;
        let srs = df.column(&column_name)?;
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

//#[tracing::instrument(name = "Obtaining DataSet Info", skip(ds))]
async fn dataset_info<DS: Serialize>(_: HttpRequest, ds: Data<DS>) -> impl Responder {
    web::Json(ds)
}

#[tracing::instrument(name = "Request Execution", skip(data))]
#[allow(clippy::if_same_then_else)]
async fn execute(
    data: Data<Arc<dyn DataSet>>,
    req: web::Json<AggregationRequest>,
    cache: Data<CACHE>,
) -> Result<HttpResponse> {
    let r = req.into_inner();
    let _cache = cache.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        // Work in progress
        if cfg!(cache) {
            // TODO change function to
            // base_engine::_execute_with_cache
            base_engine::execute_aggregation(
                r,
                Arc::clone(data.get_ref()),
                cfg!(feature = "streaming"),
            )
        } else {
            base_engine::execute_aggregation(
                r,
                Arc::clone(data.get_ref()),
                cfg!(feature = "streaming"),
            )
        }
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
#[get("/overrides")]
async fn overridable_columns(data: Data<Arc<dyn DataSet>>) -> impl Responder {
    web::Json(data.overridable_columns())
}

// TODO Why can't I use ds: impl DataSet ?
pub fn run_server(
    listener: TcpListener,
    ds: Arc<dyn DataSet>,
    _templates: Vec<AggregationRequest>,
) -> std::io::Result<Server> {
    // Read .env
    dotenv::dotenv().ok();
    // Allow pretty logs
    pretty_env_logger::init();

    let ds = Data::new(ds);
    let static_files_dir =
        std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let _templates = Data::new(_templates);

    let cache = Data::new(CACHE::new());

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
                            .service(overridable_columns)
                            .service(scenarios),
                    )
                    .route("/aggtypes", web::get().to(measures)),
            )
            // must be the last one
            .service(fs::Files::new("/", &static_files_dir).index_file("index.html"))
            .app_data(ds.clone())
            .app_data(_templates.clone())
            .app_data(cache.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}
