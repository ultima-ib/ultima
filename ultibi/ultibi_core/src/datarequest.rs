//! TODO rename to compute request and move to a separate module

use std::collections::BTreeMap;

use crate::aggregations::AggregationName;
use crate::filters::FilterE;
use crate::overrides::Override;
use crate::MeasureName;
use crate::{add_row::AdditionalRows, filters::AndOrFltrChain};

use serde::{Deserialize, Serialize};

pub type CPM = BTreeMap<String, String>;

/// Fundamentally, user might want to:
///
/// i) Aggregation: apply the same procedure to every group and get a single number
///
/// Otherwise, ii) Apply the same procedure to every group and get multiple numbers (ie a Breakdown)
// TODO #[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum ComputeRequest {
    /// Measures will be called in GroupBy-Aggregate context
    Aggregation(AggregationRequest),
    /// Converted into a Vec<AggregationRequest/Breakdown) to produce a report
    Report(ReportRequest),
    /// TODO Measures will be called in groupby-Apply Context
    Breakdown,
}

impl From<AggregationRequest> for ComputeRequest {
    fn from(item: AggregationRequest) -> Self {
        ComputeRequest::Aggregation(item)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AggregationRequest {
    // general fields
    /// Name of your request
    /// Usefull when used as a template
    #[serde(default)]
    pub name: Option<String>,
    /// Measure: (Name: String, Action: String) where Name will be looked up in
    /// MeasuresMap of the DataSet
    pub measures: Vec<(String, String)>,
    /// Which column do you want to Group By?
    pub groupby: Vec<String>,
    /// Filter your data (pre compute),
    /// See AndOrFltrChain
    #[serde(default)]
    pub filters: Vec<Vec<FilterE>>,
    #[serde(default)]
    pub overrides: Vec<Override>,
    #[serde(default, alias = "additionalRows")]
    pub add_row: AdditionalRows,
    /// Map/Dict
    #[serde(default)]
    pub calc_params: BTreeMap<String, String>,
    /// drop rows where all results are NULL or 0
    #[serde(default)]
    pub hide_zeros: bool,
    /// Show totals
    #[serde(default)]
    pub totals: bool,
}

impl AggregationRequest {
    pub fn filters(&self) -> &AndOrFltrChain {
        &self.filters
    }

    pub fn measures(&self) -> &Vec<(String, String)> {
        &self.measures
    }

    pub fn groupby(&self) -> &Vec<String> {
        &self.groupby
    }

    pub fn calc_params(&self) -> &CPM {
        &self.calc_params
    }

    pub fn overrides(&self) -> &Vec<Override> {
        &self.overrides
    }
}

/// This is used Internally as a key in Cache
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheableComputeRequest {
    /// Measures will be called in GroupBy-Aggregate context
    Aggregation(CacheableAggregationRequest),
    /// TODO Measures will be called in groupby-Apply Context
    Breakdown,
}

/// Similar to AggregationRequest, but Measure is only one
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub struct CacheableAggregationRequest {
    // general fields
    #[serde(default)]
    pub name: Option<String>,
    /// Measure: (Name, Action) where Name will be looked up in
    /// MeasuresMap of the DataSet
    pub measure: (MeasureName, AggregationName),
    pub groupby: Vec<String>,
    #[serde(default)]
    pub filters: AndOrFltrChain,
    #[serde(default)]
    pub overrides: Vec<Override>,
    #[serde(default, alias = "additionalRows")]
    pub add_row: AdditionalRows,
    #[serde(default)]
    pub calc_params: CPM,
    /// TODO potentially to move out
    #[serde(default)]
    pub totals: bool,
}

impl From<&AggregationRequest> for Vec<CacheableAggregationRequest> {
    fn from(item: &AggregationRequest) -> Self {
        item.measures()
            .iter()
            .map(|measure| CacheableAggregationRequest {
                measure: measure.clone(),
                name: item.name.clone(),
                groupby: item.groupby.clone(),
                filters: item.filters.clone(),
                overrides: item.overrides.clone(),
                add_row: item.add_row.clone(),
                calc_params: item.calc_params.clone(),
                totals: item.totals,
            })
            .collect::<Vec<CacheableAggregationRequest>>()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "type")]
pub struct ReportRequest {
    /// Report Name
    pub report_name: String,
    /// This should Deserialise into report specific request
    /// For example [GroupbyAggReportRequest]
    pub report_body: String,
}
