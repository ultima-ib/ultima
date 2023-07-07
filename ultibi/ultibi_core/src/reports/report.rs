//! TODO Work In Progress - Not ready for usage yet

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    add_row::AdditionalRows, errors::UltiResult, filters::FilterE, overrides::Override,
    AggregationRequest, ComputeRequest,
};

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

/// A report where GroupBy and Measures are fixed
#[derive(Clone)]
pub struct GroupbyAggReport {
    /// Unique name for DataSet, to be seen in the UI
    /// or called by this name from request
    pub name: String,

    /// A Report request can result in multiple [AggregationRequest]'s
    /// This is reflected by Vec<Vec<>> structure
    ///
    /// fixed_fields are used to populate outer [AggregationRequest]
    ///
    /// For this report these fields are fixed and cannot be changed
    /// For each inner Vec the FixedFields must be the same
    pub fixed_fields: Vec<Vec<FixedFields>>,

    // TODO must set fields
    /// Simply appends text for the result of each request
    pub calculator: ReportWriter,
}

/// Matches fields of [AggregationRequest]
/// Because essentially used as place holder values for [AggregationRequest]
#[derive(Clone)]
pub enum FixedFields {
    Measures(Vec<(String, String)>),
    Groupby(Vec<String>),
    Filters(Vec<Vec<FilterE>>),
    Overrides(Vec<Override>),
    AdditionalRows(AdditionalRows),
    CalcParams(BTreeMap<String, String>),
    HideZeros(bool),
    Totals(bool),
}

pub trait ReporterTrait: Send + Sync {
    /// Any Report Request
    //type Item<'a>: Deserialize<'a>;
    fn compute_request(&self, report_req: AggregationRequest) -> UltiResult<Vec<ComputeRequest>>;
    fn report(&self, dfs: &[DataFrame]) -> Report;
    fn name(&self) -> &str;
}

pub struct Reporter(pub Arc<dyn ReporterTrait>);

impl<'a> AsRef<(dyn ReporterTrait + 'a)> for Reporter {
    fn as_ref(&self) -> &(dyn ReporterTrait + 'a) {
        self.0.as_ref()
    }
}

impl Deref for Reporter {
    type Target = dyn ReporterTrait;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl From<Arc<dyn ReporterTrait>> for Reporter {
    fn from(r: Arc<dyn ReporterTrait>) -> Self {
        Self(r)
    }
}

impl FromIterator<Reporter> for ReportersMap {
    fn from_iter<I>(v: I) -> Self
    where
        I: IntoIterator<Item = Reporter>,
    {
        v.into_iter()
            .map(|reporter| (reporter.name().to_string(), reporter))
            .collect()
    }
}
