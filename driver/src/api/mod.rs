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

async fn execute(r2: HttpRequest, req: web::Json<AggregationRequest>) -> impl Responder {
    let r = req.into_inner();
    let data = r2.app_data::<Arc<dyn DataSet>>().unwrap();
    //let res = base_engine::execute_aggregation(r, Arc::clone(data)).unwrap();
    //println!("Result: {}", res);
    //web::Json(res)
    "OK"
}

pub fn run_server(listener: TcpListener, ds: impl DataSet+'static) -> std::io::Result<Server>
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
        .route("/FRTB", web::get().to(dataset_info::<Arc<dyn DataSet>>))
        .route("/FRTB", web::post().to(execute))
        .app_data(ds.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}