//! Work In Progress

use std::sync::Arc;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{errors::UltiResult, ComputeRequest};

// pub type ReportCalculator = Arc<dyn Fn(&[Expr], &CPM) -> UltiResult<Report> + Send + Sync>;

/// Returns a report
/// Writes text for each of your reports
pub type ReportWriter = Arc<dyn Fn(&[DataFrame]) -> UltiResult<Report> + Send + Sync>;

/// Customised reports about your data/results
/// For example FRTB Data Quality
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum Report {
    /// Most General report
    /// (Text, Data)
    General(Vec<(String, DataFrame)>),
}

impl Default for Report {
    fn default() -> Report {
        Report::General(vec![])
    }
}

/// Reporter produces [Report]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Reporter {
    /// Unique name for DataSet, to be seen in the UI
    pub name: String,
    /// Each report has a list of associated requests
    pub requests: Vec<ComputeRequest>,
    /// Simply appends text for the result of each request
    pub calculator: ReportWriter,
}
