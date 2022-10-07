use super::measure::OCP;
use crate::filters::AndOrFltrChain;
use crate::overrides::Overwrite;

use serde::{Deserialize, Serialize};

/// Fundamentally, user might want to:
///
/// i) Aggregation: apply the same procedure to every group and get a single number
///
/// Otherwise, ii) Apply the same procedure to every group and get multiple numbers (ie a Breakdown)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataRequestE {
    /// Measures will be called in GroupBy-Aggregate context
    Aggregation(AggregationRequest),
    /// TODO Measures will be called in groupby-Apply Context
    Breakdown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub struct AggregationRequest {
    // general fields
    #[serde(default)]
    name: Option<String>,
    /// Measure: (Name, Action) where Name will be looked up in
    /// MeasuresMap of the DataSet
    measures: Vec<(String, String)>,
    groupby: Vec<String>,
    #[serde(default)]
    filters: AndOrFltrChain,
    #[serde(default)]
    overwrites: Vec<Overwrite>,
    #[serde(default)]
    calc_params: OCP,
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

    pub fn _groupby(&self) -> &Vec<String> {
        &self.groupby
    }

    pub fn calc_params(&self) -> &OCP {
        &self.calc_params
    }

    pub fn overrides(&self) -> &Vec<Overwrite> {
        &self.overwrites
    }
}
