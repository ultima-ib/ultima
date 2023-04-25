//! Work In Progress

use std::sync::Arc;

use polars::{prelude::DataFrame, lazy::dsl::Expr};
use serde::{Deserialize, Serialize};

use crate::{CPM, errors::{UltiResult}};

pub type ReportCalculator = Arc<dyn Fn(&CPM) -> UltiResult<Expr> + Send + Sync>;

/// Customised reports about your data/results
/// For example FRTB Data Quality
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum Report {
    /// Most General report
    /// (Text, Data)
    General(Vec<(String, DataFrame)>)
}

impl Default for Report {
    fn default() -> Report {
        Report::General(vec![])
    }
}