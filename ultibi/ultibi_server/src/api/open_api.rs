//! This is Work in progress

//use std::sync::{RwLock, Arc};
//use serde::{Serialize, Deserialize};
//use ultibi_core::{DataSet, DataFrame, ComputeRequest};
//use utoipa::{OpenApi, ToSchema};
use crate::api::routers;
use ultibi_core::{AggregationRequest, filters::FilterE};
use utoipa::OpenApi;

//use super::routers::execute;
//use crate::api::routers::__path_execute;
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
