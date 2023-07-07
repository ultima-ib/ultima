use std::sync::Arc;

use polars::prelude::{DataFrame, LazyFrame, IntoLazy, Schema};
use serde::{Serialize, Deserialize};

use crate::{filters::{AndOrFltrChain, fltr_chain}, errors::UltiResult};


/// Indicated the source of data
pub enum DataSource {
    /// In Memory Data - fast, since prepare runs only once, instead of in every request
    InMemory(DataFrame),
    /// It's caller's responsibility to ensure that this Frame is a Scan and not just any LazyFrame
    Scan(LazyFrame),
    // TODO DB Conn
}

/// Maps to [Source]
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(untagged)]
pub enum SourceVariant {
    #[default]
    InMemory,
    Scan,
    // TODO DB Conn
}

/// Marker trait implementation to ensure every SourceVariant is covered
impl From<DataSource> for SourceVariant{
    fn from(item: DataSource) -> Self {
        match item {
            DataSource::InMemory(_) => SourceVariant::InMemory,
            DataSource::Scan(_) => SourceVariant::Scan,
        }
    }
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::InMemory(Default::default())
    }
}

impl DataSource {

    pub fn get_lazyframe(&self, filters: &AndOrFltrChain) -> LazyFrame{
        let filter = fltr_chain(filters);
        match self {
            DataSource::InMemory(df) => if let Some(f) = filter { df.clone().lazy().filter(f) } else {df.clone().lazy()},
            DataSource::Scan(lf) => if let Some(f) = filter { lf.clone().filter(f) } else {lf.clone()}
        }
    }
    pub fn get_schema(&self) -> UltiResult<Arc<Schema>>{
        match self {
            DataSource::InMemory(df) => Ok(Arc::new(df.schema())),
            DataSource::Scan(lf) => Ok(lf.schema()?)
        }
    }

    /// InMemory -> false
    /// Scan -> true
    pub fn prepare_on_each_request(&self) -> bool{
        match self {
            DataSource::InMemory(_) => false,
            DataSource::Scan(_) => true
        }
    }
}