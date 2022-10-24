use std::sync::Arc;
use dashmap::DashMap;

use polars::prelude::{DataFrame, PolarsResult, col, Expr, JoinType};
use polars::prelude::IntoLazy;

pub type CACHE = DashMap::<AggregationRequest, DataFrame>;

use crate::{DataSet, AggregationRequest};

/// TODO work in progress
pub(crate) fn _execute_with_cache(data: Arc<dyn DataSet>,
    req: AggregationRequest,
    cache: CACHE) -> PolarsResult<DataFrame>{
        let requested_measures = req.measures().clone();
        let grp_by_expr = req._groupby().iter().map(|x|col(x)).collect::<Vec<Expr>>();
        //let order = req.measures().iter().map(|(name, _)|name.as_str()).collect::<Vec<&str>>();

        // have already been calculated
        let mut cached_res = vec![]; 
        // Need to be calculated
        let mut new = vec![];
        // for each measure in req check cache
        for m in requested_measures {
          let sub_req =   AggregationRequest{measures: vec![m.clone()], ..req.clone()};
          // checking cache
          match cache.get(&sub_req) {
            // If found - store result
            Some(rf) => {
                cached_res.push(rf.value().clone());
            },
            // if not push to those which will have to be calculated
            _ => new.push(m),
          }
        }

        let mut res: DataFrame;

        let chached_df = if !cached_res.is_empty() {
            let mut it = cached_res.into_iter();
            let mut res = it.next().unwrap(); //cached_res is not empty
            for df in it {
              res = res.lazy()
                .join(df.lazy(), grp_by_expr.clone(), grp_by_expr.clone(), JoinType::Outer)
                .collect()?
            };
            Some(res)
        }else{None};

        let new_res = if !new.is_empty() {
          let new_req = AggregationRequest{measures: new, ..req.clone()};
          let new_res = super::execute_aggregation(new_req, data)?;
          Some(new_res)
        }else {None};
        // if found -> push to storage ; if not found -> push to not found
        // storage_joined = outer join storage on groupby

        // res = execute not found

        // res = outer join res with storage_joined
        unimplemented!()
    }