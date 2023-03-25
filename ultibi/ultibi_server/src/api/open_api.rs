//! This is Work in progress

//use std::sync::{RwLock, Arc};
//use serde::{Serialize, Deserialize};
//use ultibi_core::{DataSet, DataFrame, ComputeRequest};
//use utoipa::{OpenApi, ToSchema};
use crate::api::routers;
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
    ),
    //components(
    //    schemas(ComputeRequestAPI, DataSetAPI, DataFrameAPI)
    //),
    tags(
        (name = "Ultima BI", description = "Ultimate Business Intellegence endpoints.")
    ),
)]
pub(crate) struct ApiDoc;
