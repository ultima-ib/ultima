use actix_web::{web, App, HttpRequest, HttpServer, Responder, HttpResponse, get, dev::Server};
use std::net::TcpListener;

async fn greet(req: HttpRequest) -> impl Responder {
    let name = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", &name)
}

#[get("/health_check")]
async fn health_check(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}  

async fn dataset_info(req: HttpRequest) -> impl Responder {
    let name = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", &name)
}

pub fn run_server(listener: TcpListener) -> std::io::Result<Server> {
    let server = HttpServer::new(|| {
        App::new()
        .service(health_check)
        .route("/", web::get().to(greet))
        .route("/{name}", web::get().to(greet))
        .route("/FRTB", web::get().to(dataset_info))
    })
    .listen(listener)?
    .run();
    Ok(server)
}