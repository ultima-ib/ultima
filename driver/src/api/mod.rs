//! This module builds App and is Server bin specific
#[cfg(feature = "FRTB")]
use frtb_engine::statics::MEDIUM_CORR_SCENARIO;

use actix_web::{
    dev::Server,
    dev::ServiceRequest,
    get,
    middleware::Logger,
    web::{self, Data},
    App,
    HttpRequest,
    HttpResponse,
    HttpServer,
    Responder,
    //error::InternalError, http::StatusCode,
    Result, http::header::ContentType,
};
use actix_web_httpauth::extractors::basic::BasicAuth;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::task;

use ultibi::{
    aggregations::BASE_CALCS, col, polars::prelude::PolarsError, prelude::PolarsResult,
    AggregationRequest, DataFrame, DataSet,
};

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
        let lf = d.get_lazyframe();
        let df = lf.clone().select([col(&column_name)]).collect()?;
        let srs = df.column(&column_name)?;
        let search = ultibi::helpers::searches::filter_contains_unique(srs, &pat)?;
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

#[tracing::instrument(name = "Describe", skip(jdf))]
async fn describe(jdf: web::Json<DataFrame>) -> Result<HttpResponse> {
    let df = jdf.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || df.describe(None))
        .await
        .context("Failed to spawn blocking task.")
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json(res))
}

#[tracing::instrument(name = "Request Execution", skip(data))]
#[allow(clippy::if_same_then_else)]
async fn execute(
    data: Data<Arc<dyn DataSet>>,
    req: web::Json<AggregationRequest>,
) -> Result<HttpResponse> {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        // Work in progress
        if cfg!(cache) {
            #[cfg(feature = "cache")]
            return ultibi::execute_agg_with_cache::execute_with_cache(
                &r,
                &*Arc::clone(data.get_ref()),
                cfg!(feature = "streaming"),
            );
            Err(PolarsError::NoData("Cache must be enabled.".into()))
        } else {
            ultibi::exec_agg(&*Arc::clone(data.get_ref()), r, cfg!(feature = "streaming"))
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

#[get("/")]
async fn ui() -> impl Responder {
    // This works but not on docker let index = include_str!(r"../../../frontend/dist/index.html");
    let index = include_str!(r"index.html");

    HttpResponse::Ok()
      .content_type(ContentType::html())
      .body(index)
}

async fn _validator(
    req: ServiceRequest,
    creds: BasicAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    let user_id = creds.user_id();
    let password = creds.password();

    if user_id == "ultima" && password == Some("password123!!!") {
        return Ok(req);
    }
    let error = actix_web::error::ErrorUnauthorized("invalid credentions!");
    Err((error, req))
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
    //let static_files_dir =
    //    std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let _templates = Data::new(_templates);

    let server = HttpServer::new(move || {
    //let auth = HttpAuthentication::basic(validator);

    App::new()
        .wrap(Logger::default())
        //.wrap(auth)
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
                .route("/aggtypes", web::get().to(measures))
                .route("/describe", web::post().to(describe)),
        )
        // must be the last one
        //.service(fs::Files::new("/", &static_files_dir).index_file("index.html"))
        .service(ui )
        .app_data(ds.clone())
        .app_data(_templates.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}

fn _workspace_dir() -> PathBuf {
    let output = std::process::Command::new(env!("CARGO"))
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format=plain")
        .output()
        .unwrap()
        .stdout;
    let cargo_path = Path::new(std::str::from_utf8(&output).unwrap().trim());
    cargo_path.parent().unwrap().to_path_buf()
}
