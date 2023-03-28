//! This is Work in progress

use crate::api::routers;
use ultibi_core::{filters::FilterE, AggregationRequest};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Ultima BI"
    ),
    paths(
        routers::execute,
        routers::column_search,
        routers::dataset_info,
        routers::templates,
        routers::overridable_columns,
        routers::aggtypes,
        routers::describe,
    ),
    components(
        schemas(AggregationRequest, FilterE)
    ),
    tags(
        (name = "Ultima BI", description = "Ultimate Business Intellegence endpoints.")
    ),
)]
pub(crate) struct ApiDoc;
