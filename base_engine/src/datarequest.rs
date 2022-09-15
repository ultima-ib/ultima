use super::measure::OptParams;
use crate::filters::*;
use crate::overrides::Override;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Fundamentally, user might want to:
/// 
/// i) Aggregation: apply the same procedure to every group and get a single number
/// 
/// Otherwise, ii) Apply the same procedure to every group and get multiple numbers (ie a Breakdown)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum  DataRequestE {
    /// Measures will be called in GroupBy-Aggregate context
    Aggregation(AggregationRequest),
    /// Measures will be called in groupby-Apply Context
    Breakdown
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggregationRequest {
    // general fields
    /// Measure can be of two types:
    /// basic: Column - Action
    /// bespoke: DerivedField - Action
    measures: Vec<(String, String)>,
    groupby: Vec<String>,
    filters: Vec<FilterE>,
    #[serde(default)]
    overrides: Vec<Override>,
    #[serde(default)]
    optional_params: Option<OptParams>,

}

impl AggregationRequest {
    /// returns a Vec of columns required by current request
    /// as well as bespoke_measures vec (eg FXDeltaSensWeighted) from the request
    /// not required due to depreciated filtering
    /// TODO remove
    pub fn required_columns(
        &self,
        bespoke_measure_col_map: Arc<HashMap<String, Vec<String>>>,
    ) -> (Vec<String>, Vec<String>) {
        //We have to clone, because required_column come not only from the request, but also
        //from bespoke_measure_col_map, and we want to preserve DataRequest
        //@TODO if possible, optimize to use &str later

        let mut res: Vec<String> = Vec::new();

        // each of the groupby cols must be present
        for i in &self.groupby {
            res.push(i.into())
        }

        // each filter column must be present
        for f in &self.filters {
            match f {
                FilterE::Eq(v) | FilterE::Neq(v) => {
                    for s in v {
                        res.push(s.0.clone())
                    }
                }
                FilterE::In(v) | FilterE::NotIn(v) => {
                    for s in v {
                        res.push(s.0.clone())
                    }
                }
            }
        }

        let mut bespoke_measures_from_request: Vec<String> = Vec::new();

        //Finally, each
        for m in &self.measures {
            match bespoke_measure_col_map.get(&*m.0) {
                Some(x) => {
                    bespoke_measures_from_request.push(m.0.clone());
                    res.extend(x.iter().map(|x| x.into()));
                }
                _ => res.push(m.0.clone()),
            }
        }

        res.sort_unstable();
        res.dedup();
        (res, bespoke_measures_from_request)
    }

    pub fn filters(&self) -> &Vec<FilterE> {
        &self.filters
    }

    pub fn measures(&self) -> &Vec<(String, String)> {
        &self.measures
    }

    pub fn _groupby(&self) -> &Vec<String> {
        &self.groupby
    }

    pub fn optiona_params(&self) -> &Option<OptParams> {
        &self.optional_params
    }
}
