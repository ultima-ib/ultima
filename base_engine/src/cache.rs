use polars::prelude::DataFrame;

use crate::AggregationRequest;

pub type Cache = dashmap::DashMap<AggregationRequest, DataFrame>;

/// Represents a DataSet (Struct) with cache
/// 
pub trait Cacheable {
    /// Gets the cache
    fn get_cache(&self) -> &Cache;
    /// Get mutable cache
    fn get_cache_mut(&mut self) -> &mut Cache;
    /// Cleans cache
    fn clean_cache(&mut self) {
        self.get_cache().clear()
    }

}