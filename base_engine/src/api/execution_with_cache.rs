use once_cell::sync::Lazy;
use polars::prelude::{DataFrame, PolarsError, PolarsResult};
use schnellru::{ByLength, LruMap};
use std::sync::RwLock;

use crate::{execute_aggregation, AggregationRequest, DataSet};

// TODO: may be replace to ByMemoryUsage?
static GLOBAL_CACHE: Lazy<RwLock<LruMap<AggregationRequest, DataFrame>>> =
    Lazy::new(|| RwLock::new(LruMap::new(ByLength::new(20))));

// TODO work in progress
pub fn execute_with_cache<DS: DataSet + ?Sized>(
    req: &AggregationRequest,
    data: &DS,
    streaming: bool,
) -> PolarsResult<DataFrame> {
    // Get Write lock on Read cause modify cashe range
    if let Ok(mut cache) = GLOBAL_CACHE.write() {
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
