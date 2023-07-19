use actix_web::{dev::Server, middleware::Logger, web::Data, App, HttpServer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
};

use ultibi_core::{AggregationRequest, DataSet};

use actix_web_static_files::ResourceFiles;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

use crate::api::{open_api::ApiDoc, routers};
pub fn build_app(
    listener: TcpListener,
    ds: Arc<RwLock<dyn DataSet>>,
    _templates: Vec<AggregationRequest>,
) -> std::io::Result<Server> {
    let ds = Data::from(ds);
    //let streaming = Data::new(streaming);
    let openapi = ApiDoc::openapi();

    let _templates = Data::new(_templates);

    let server = HttpServer::new(move || {
        let generated = generate();

        App::new()
            .wrap(Logger::default())
            .configure(routers::configure())
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi.clone()),
            )
            .service(ResourceFiles::new("/", generated))
            .app_data(ds.clone())
            .app_data(_templates.clone())
        //.app_data(streaming.clone())
    })
    .listen(listener)?
    .run();
    Ok(server)
}
