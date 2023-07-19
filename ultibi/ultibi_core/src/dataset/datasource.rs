use std::sync::Arc;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Schema};
use serde::{Deserialize, Serialize};

use crate::{
    errors::UltiResult,
    filters::{fltr_chain, AndOrFltrChain},
};

/// Indicated the source of data
#[derive(Clone)]
pub enum DataSource {
    /// In Memory Data - fast, since prepare runs only once, instead of in every request
    InMemory(DataFrame),
    /// It's caller's responsibility to ensure that this Frame is a Scan and not just any LazyFrame
    Scan(LazyFrame),
    // TODO DB Connection
    #[cfg(feature = "db")]
    Db(DbInfo),
}

/// Maps to [Source]
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(untagged)]
pub enum SourceVariant {
    #[default]
    InMemory,
    Scan,
    // TODO DB Conn
    #[cfg(feature = "db")]
    Db,
}

#[derive(Clone)]
pub struct DbInfo;

/// Marker trait implementation to ensure every SourceVariant is covered
impl From<DataSource> for SourceVariant {
    fn from(item: DataSource) -> Self {
        match item {
            DataSource::InMemory(_) => SourceVariant::InMemory,
            DataSource::Scan(_) => SourceVariant::Scan,
            #[cfg(feature = "db")]
            DataSource::Db(_) => SourceVariant::Db,
        }
    }
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::InMemory(Default::default())
    }
}

impl DataSource {
    pub fn get_lazyframe(&self, filters: &AndOrFltrChain) -> LazyFrame {
        let filter = fltr_chain(filters);
        match self {
            DataSource::InMemory(df) => {
                if let Some(f) = filter {
                    df.clone().lazy().filter(f)
                } else {
                    df.clone().lazy()
                }
            }
            DataSource::Scan(lf) => {
                if let Some(f) = filter {
                    lf.clone().filter(f)
                } else {
                    lf.clone()
                }
            }
            #[cfg(feature = "db")]
            DataSource::Db(_) => todo!(),
        }
    }
    pub fn get_schema(&self) -> UltiResult<Arc<Schema>> {
        match self {
            DataSource::InMemory(df) => Ok(Arc::new(df.schema())),
            DataSource::Scan(lf) => Ok(lf.schema()?),
            #[cfg(feature = "db")]
            DataSource::Db(_) => todo!(),
        }
    }

    /// InMemory -> false
    /// Scan -> true
    /// Db -> true
    pub fn prepare_on_each_request(&self) -> bool {
        match self {
            DataSource::InMemory(_) => false,
            DataSource::Scan(_) => true,
            #[cfg(feature = "db")]
            DataSource::Db(_) => unimplemented!(),
        }
    }
}

impl From<DataFrame> for DataSource {
    fn from(item: DataFrame) -> Self {
        DataSource::InMemory(item)
    }
}

impl From<LazyFrame> for DataSource {
    fn from(item: LazyFrame) -> Self {
        DataSource::Scan(item)
    }
}
