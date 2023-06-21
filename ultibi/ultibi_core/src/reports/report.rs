//! Work In Progress

use std::{sync::Arc, collections::BTreeMap};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{errors::UltiResult, ComputeRequest};

// pub type ReportCalculator = Arc<dyn Fn(&[Expr], &CPM) -> UltiResult<Report> + Send + Sync>;

/// Returns a report
/// Writes text for each of your reports
pub type ReportWriter = Arc<dyn Fn(&[DataFrame]) -> UltiResult<Report> + Send + Sync>;
/// (Reporter Name, Reporter)
pub type ReportersMap = BTreeMap<ReporterName, Reporter>;

/// Each [DataSet] has reporters accessed via get_reporters()
/// This alias to represent a Reporter name, a unique string
pub type ReporterName = String;

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

/// Publishes [Report]
#[derive(Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Reporter {
    /// Unique name for DataSet, to be seen in the UI
    /// or called by this name from request
    pub name: String,

    /// Each report has a list of associated !incomplete! requests
    /// incomplete means GroupBy, Measure is fixed
    /// everything else is variable
    pub requests: Vec<ComputeRequest>,

    /// Simply appends text for the result of each request
    pub calculator: ReportWriter,
}


/// Publishes a [Report]
#[derive(Clone)]
//#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum Publisher {
    /// This report has fixed GroupBy, Measure
    /// Everything else is ammendable
    /// eg FRTB EQ11
    GroupbyAggPublisher(GroupbyAggReport)  
}

/// Publishes [Report]
#[derive(Clone)]
//#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GroupbyAggReport {
    /// Unique name for DataSet, to be seen in the UI
    /// or called by this name from request
    pub name: String,

    /// Each report has a list of associated !incomplete! requests
    /// incomplete means GroupBy, Measure is fixed
    /// everything else is variable
    pub requests: Vec<GroupbyAggReportRequest>,

    /// Simply appends text for the result of each request
    pub calculator: ReportWriter,
}

#[derive(Clone)]
pub struct GroupbyAggReportRequest;





