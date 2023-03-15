use actix_web::{
    dev::Server,
    get,
    //http::header::ContentType,
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
use serde::Deserialize;
use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
};
use tokio::task;

use ultibi_core::{
    aggregations::BASE_CALCS, col, prelude::PolarsResult, AggregationRequest, DataFrame, DataSet,
};

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
    data: Data<RwLock<dyn DataSet>>,
) -> Result<HttpResponse> {
    let column_name = path.into_inner();
    let (page, pat) = (page.page, page.pattern.clone());
    let res = task::spawn_blocking(move || {
        let lf = data
            .read()
            .expect("Poisonned RwLock")
            .get_lazyframe()
            .clone();
        let df = lf.select([col(&column_name)]).collect()?;
        let srs = df.column(&column_name)?;
        let search = ultibi_core::helpers::searches::filter_contains_unique(srs, &pat)?;
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
async fn dataset_info(_: HttpRequest, ds: Data<RwLock<dyn DataSet>>) -> impl Responder {
    let a = ds.read().unwrap();
    let body = serde_json::to_string(&*a).unwrap();
    //web::Json(&*a)
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .message_body(body)
}

#[tracing::instrument(name = "Describe", skip(jdf))]
async fn describe(jdf: web::Json<DataFrame>) -> Result<HttpResponse> {
    let df = jdf.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || crate::helpers::describe(df, None))
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

#[tracing::instrument(name = "Request Execution", skip(data))]
async fn execute(
    data: Data<RwLock<dyn DataSet>>,
    req: web::Json<AggregationRequest>,
    streaming: Data<bool>,
) -> Result<HttpResponse> {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || {
        data.read()
            .expect("Poisonned RwLock")
            .compute(r.into(), **streaming) //TODO streaming mode
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
async fn overridable_columns(data: Data<RwLock<dyn DataSet>>) -> impl Responder {
    web::Json(data.read().expect("Poisonned RwLock").overridable_columns())
}

use actix_web_static_files::ResourceFiles;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

//<DS: DataSet + 'static + ?Sized>
pub fn build_app(
    listener: TcpListener,
    ds: Arc<RwLock<dyn DataSet>>,
    _templates: Vec<AggregationRequest>,
    streaming: bool,
) -> std::io::Result<Server> {
    let ds = Data::from(ds);
    let streaming = Data::new(streaming);
    //let static_files_dir =
    //    std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let _templates = Data::new(_templates);

    let server = HttpServer::new(move || {
        let generated = generate();

        App::new()
            .wrap(Logger::default())
            //.wrap(auth)
            .service(
                web::scope("/api")
                    .service(health_check)
                    .service(
                        web::scope("/FRTB")
                            //.route("", web::get().to(dataset_info::<Arc<dyn DataSet>>))
                            .route("", web::get().to(dataset_info))
                            .route("", web::post().to(execute))
                            .service(column_search)
                            .service(templates)
                            .service(overridable_columns),
                    )
                    .route("/aggtypes", web::get().to(measures))
                    .route("/describe", web::post().to(describe)),
            )
            // must be the last one
            .service(ResourceFiles::new("/", generated))
            .app_data(ds.clone())
            .app_data(_templates.clone())
            .app_data(streaming.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}
