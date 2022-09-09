use actix_web::{web, App, HttpRequest, HttpServer, Responder, HttpResponse, get};

async fn greet(req: HttpRequest) -> impl Responder {
    let name = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", &name)
}
#[get("/health_check")]
async fn health_check(req: HttpRequest) -> impl Responder {
    HttpResponse::Ok()
}    

#[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
        .service(health_check)
        .route("/", web::get().to(greet))
        .route("/{name}", web::get().to(greet))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}