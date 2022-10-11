use std::collections::BTreeMap;

use super::measure::OCP;
use crate::filters::AndOrFltrChain;
use crate::overrides::Override;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub struct AggregationRequest {
    // general fields
    #[serde(default)]
    pub name: Option<String>,
    /// Measure: (Name, Action) where Name will be looked up in
    /// MeasuresMap of the DataSet
    pub measures: Vec<(String, String)>,
    pub groupby: Vec<String>,
    #[serde(default)]
    pub filters: AndOrFltrChain,
    #[serde(default)]
    pub overrides: Vec<Override>,
    #[serde(default)]
    pub calc_params: OCP,
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

    pub fn overrides(&self) -> &Vec<Override> {
        &self.overrides
    }
}

use std::hash::{Hash, Hasher};

impl Hash for AggregationRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.measures.hash(state);
        self.groupby.hash(state);
        self.filters.hash(state);
        self.overrides.hash(state);
        self.hide_zeros.hash(state);
        self.totals.hash(state);
        //Hashmap is only hashable via BTreeMap
        self.calc_params.iter().collect::<BTreeMap<_, _>>().hash(state);
    }
}

/// This struct is a Helper to allow hashing [AggregationRequest] 
/// Note [AggregationRequest].calc_params is a Hashmap and hence is not Hashable
#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
#[serde(tag = "type")]
struct HashableAggregationRequest {
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
    overrides: Vec<Override>,
    #[serde(default)]
    calc_params: BTreeMap<String, String>,
    /// drop rows where all results are NULL or 0
    #[serde(default)]
    pub hide_zeros: bool,
    /// Show totals
    #[serde(default)]
    pub totals: bool,
}
