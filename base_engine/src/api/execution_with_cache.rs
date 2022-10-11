use std::sync::Arc;
use dashmap::DashMap;

use polars::prelude::{DataFrame, PolarsResult};
pub type CACHE = DashMap::<AggregationRequest, DataFrame>;

use crate::{DataSet, AggregationRequest};

pub(crate) fn execute_with_cache(data: Arc<dyn DataSet>,
    req: AggregationRequest,
    cache: CACHE) -> PolarsResult<DataFrame>{
        let requested_measures = req.measures().clone();
        //let order = req.measures().iter().map(|(name, _)|name.as_str()).collect::<Vec<&str>>();

        // have already been calculated
        let mut cached_res = vec![]; 
        // Need to be calculated
        let mut new = vec![];
        // for each measure in req check cache
        for m in requested_measures {
          let _req =   AggregationRequest{measures: vec![m.clone()], ..req.clone()};
          // checking cache
          match cache.get(&req) {
            // If found - store result
            Some(rf) => {
                cached_res.push(rf.value().clone());
            },
            // if not push to those which will have to be calculated
            _ => new.push(m),
          }
        }

        let mut res: DataFrame;

        let new_res = if !new.is_empty() {
          let new_req = AggregationRequest{measures: new, ..req.clone()};
          super::execute_aggregation(new_req, data)?
        }else {Default::default()};
        // if found -> push to storage ; if not found -> push to not found
        // storage_joined = outer join storage on groupby

        // res = execute not found

        // res = outer join res with storage_joined
        unimplemented!()
    }