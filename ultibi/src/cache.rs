use polars::prelude::DataFrame;

use crate::{CacheableComputeRequest, DataSet, DataSetBase};

pub type Cache = dashmap::DashMap<CacheableComputeRequest, DataFrame>;

/// Represents a DataSet (Struct) with cache
/// We recommend implementing Cacheable for your DataSet
/// note that you must also set as_cacheable of DataSet.
pub trait CacheableDataSet: DataSet + Send + Sync {
    /// Gets the cache
    fn get_cache(&self) -> &Cache;

    /// Cleans cache
    fn clean_cache(&self) {
        self.get_cache().clear()
    }
}

impl CacheableDataSet for DataSetBase {
    /// Gets the cache
    fn get_cache(&self) -> &Cache {
        &self.cache
    }
}
