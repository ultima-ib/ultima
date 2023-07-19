use std::sync::RwLock;

use actix_web::{
    get, post,
    web::{self, Data, ServiceConfig},
    HttpRequest, HttpResponse, Responder, Result,
};
use anyhow::Context;
use serde::Deserialize;
use tokio::task;
use ultibi_core::{
    aggregations::BASE_CALCS, errors::UltiResult, AggregationRequest, ComputeRequest, DataFrame,
    DataSet,
};
use utoipa::IntoParams;

#[derive(Deserialize, IntoParams)]
struct Pagination {
    page: usize,
    pattern: String,
}
const PER_PAGE: u16 = 100;

#[get("/health_check")]
async fn health_check(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}

#[utoipa::path]
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
        let srs = data
            .read()
            .expect("Poisonned RwLock")
            .get_column(&column_name)?;

        let search = ultibi_core::helpers::searches::filter_contains_unique(&srs, &pat)?;
        let first = page * PER_PAGE as usize;
        let last = first + PER_PAGE as usize;
        let s = search.slice(first as i64, last);
        UltiResult::Ok(s)
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
#[utoipa::path(get)]
#[get("")]
async fn dataset_info(_: HttpRequest, ds: Data<RwLock<dyn DataSet>>) -> impl Responder {
    let a = ds.read().unwrap();
    let body = serde_json::to_string(&*a).unwrap();
    //web::Json(&*a)
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .message_body(body)
}

#[utoipa::path(
    post,
    request_body(content = AggregationRequest, description = "What do you want to calculate", content_type = "application/json",
        example = json!(r#"
        {   "filters": [{"op":"Eq", "field":"Group", "value":"Ultima"}],
    
            "groupby": ["RiskClass", "Desk"],
            
            "overrides": [{   "field": "SensWeights",
                              "value": "[0.005]",
                              "filters": [
                                        [{"op":"Eq", "field":"RiskClass", "value":"DRC_nonSec"}],
                                        [{"op":"Eq", "field":"CreditQuality", "value":"AA"}]
                                        ]
                        }],
            
            "measures": [
                ["DRC nonSec CapitalCharge", "scalar"]
                    ],
            "type": "AggregationRequest",
            
            "hide_zeros": true,
            "calc_params": {
                "jurisdiction": "BCBS",
                "apply_fx_curv_div": "true",
                "drc_offset": "false"
            }}
    "#)
    ),
    responses(
        (status = 200, description = "Result of the compute request",body = DataFrame,
         content_type = "application/json", 
         example=json!(
            r#"{"columns":[{"name":"RiskCategory","datatype":"Utf8","values":["DRC","Vega","Delta"]},{"name":"COB","datatype":"Utf8","values":["22/07/2022","22/07/2022","22/07/2022"]},{"name":"SA Charge","datatype":"Float64","values":[12777.688636772913,417064.5099482173,169292.7255377446]}]}"#
        ))
    )
)]
#[tracing::instrument(name = "Request Execution", skip(data))]
#[post("")]
pub(crate) async fn execute(
    data: Data<RwLock<dyn DataSet>>,
    req: web::Json<ComputeRequest>,
) -> Result<HttpResponse> {
    let r = req.into_inner();
    // TODO kill this OS thread if it is hanging (see spawn_blocking docs for ideas)
    let res = task::spawn_blocking(move || data.read().expect("Poisonned RwLock").compute(r))
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
#[utoipa::path]
#[get("/templates")]
async fn templates(_: HttpRequest, templates: Data<Vec<AggregationRequest>>) -> impl Responder {
    web::Json(templates)
}
#[utoipa::path(get)]
#[get("/overrides")]
async fn overridable_columns(data: Data<RwLock<dyn DataSet>>) -> impl Responder {
    web::Json(data.read().expect("Poisonned RwLock").overridable_columns())
}

pub fn configure() -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config.service(
            web::scope("/api")
                .service(aggtypes)
                .service(describe)
                .service(health_check)
                //TODO change FRTB to DataSet
                .service(
                    web::scope("/FRTB")
                        .service(dataset_info)
                        .service(execute)
                        .service(column_search)
                        .service(templates)
                        .service(overridable_columns),
                ),
        );
    }
}

// Not Ultibi DataSet Specific
#[utoipa::path]
#[get("/aggtypes")]
async fn aggtypes() -> impl Responder {
    let res = BASE_CALCS.iter().map(|(x, _)| *x).collect::<Vec<&str>>();
    web::Json(res)
}

#[utoipa::path]
#[tracing::instrument(name = "Describe", skip(jdf))]
#[post("/describe")]
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
