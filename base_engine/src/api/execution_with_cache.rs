use dashmap::DashMap;
use once_cell::sync::Lazy;
use polars::prelude::{DataFrame, PolarsError, PolarsResult};
use std::sync::RwLock;

use crate::{execute_aggregation, AggregationRequest, DataSet};

static GLOBAL_CACHE: Lazy<RwLock<DashMap<AggregationRequest, DataFrame>>> =
    Lazy::new(|| RwLock::new(DashMap::new()));

// TODO work in progress
pub fn execute_with_cache<DS: DataSet + ?Sized>(
    req: &AggregationRequest,
    data: &DS,
    streaming: bool,
) -> PolarsResult<DataFrame> {
    if let Ok(cache) = GLOBAL_CACHE.read() {
        let cached_result = cache.get(req);
        if let Some(cached) = cached_result {
            Ok(cached.clone())
        } else {
            let val = execute_aggregation(req, data, streaming)?;
            cache.insert(req.clone(), val.clone());
            Ok(val)
        }
    } else {
        Err(PolarsError::NoData("Cache didnt exist.".into()))
    }
}
