use crate::filters::*;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Hash)]
pub struct DataRequestS { 
    // general fields
    /// Measure can be of two types:
    /// basic: Column - Action
    /// bespoke: DerivedField - Action
    measures: Vec<(String, String)>,
    groupby: Vec<String>,
    filters: Vec<FilterS>,
    #[serde(default="empty_params")]
    optional_params: Vec<(String, String)>  //<--> this is to accomodate for FRTBParams
}

fn empty_params() -> Vec<(String, String)> {
    Vec::new()
}

impl DataRequestS {
    /// returns a Vec of columns required by current request
    /// as well as bespoke_measures vec (eg FXDeltaSensWeighted) from the request
    pub fn required_columns(&self, bespoke_measure_col_map: Arc<HashMap<String, Vec<String>>>) -> 
    (Vec<String>, Vec<String>) {
        //We have to clone, because required_column come not only from the request, but also
        //from bespoke_measure_col_map, and we want to preserve DataRequest
        //@TODO if possible, optimize to use &str later
        
        let mut res: Vec<String> = Vec::new();

        // each of the groupby cols must be present
        for i in &self.groupby {
            res.push(i.into())
        }

        // each filter column must be present
        for f in &self.filters{
            match f{
                FilterS::Eq(v) | FilterS::Neq(v) => { 
                    for s in v {
                        res.push(s.0.clone())
                }},
                FilterS::In(v) | FilterS::NotIn(v) => { 
                    for s in v {
                        res.push(s.0.clone())
                }},
            }
        }

        let mut bespoke_measures_from_request: Vec<String> = Vec::new();

        //Finally, each 
        for m in &self.measures {
            match bespoke_measure_col_map.get(&*m.0) {
                Some(x) => {
                    bespoke_measures_from_request.push(m.0.clone());
                    res.extend(x.iter().map(|x| x.into())); },
                _ => res.push(m.0.clone()),
            }
        };
        

        res.sort_unstable();
        res.dedup();
        (res, bespoke_measures_from_request)
    }

    fn opt_params(&self) -> HashMap<String, String> {
        let hm = self.optional_params
        .clone()
        .into_iter()
        .collect();
        hm
    }

    pub fn filters(&self) -> &Vec<FilterS> {
        &self.filters
    }

    pub fn measures(&self) -> &Vec<(String, String)> {
        &self.measures
    }

    pub fn _groupby(&self) -> &Vec<String> {
        &self.groupby
    }
}