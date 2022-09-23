pub mod acquire;

use actix_web::{web::{self, Data}, App, HttpRequest, HttpServer, Responder, HttpResponse, get, dev::Server, middleware::Logger};
use base_engine::{DataSet, AggregationRequest};
use serde::Serialize;
use std::{net::TcpListener, sync::Arc};


//async fn greet(req: HttpRequest) -> impl Responder {
//    let name = req.match_info().get("name").unwrap_or("World");
//    format!("Hello {}!", &name)
//}

#[get("/health_check")]
async fn health_check(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}  

async fn dataset_info<DS: Serialize>(_: HttpRequest, ds: Data<DS>) -> impl Responder {
    web::Json(ds)
}

async fn execute(data: Data<Arc<dyn DataSet>>, req: web::Json<AggregationRequest>) -> impl Responder {
    let r = req.into_inner();
    let data = data.get_ref();
    let res = base_engine::execute_aggregation(r, Arc::clone(data)).unwrap();
    web::Json(res)
}

// TODO Why 'static here? ds is not static!
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
        //.route("/", web::get().to(greet))
        //.route("/{name}", web::get().to(greet))
        .route("/api/FRTB", web::get().to(dataset_info::<Arc<dyn DataSet>>))
        .route("/api/FRTB", web::post().to(execute))
        .app_data(ds.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}