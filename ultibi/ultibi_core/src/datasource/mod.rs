#[cfg(feature = "db")]
pub mod db;
#[cfg(feature = "db")]
pub mod db_utils;

use std::sync::Arc;

use polars::{
    lazy::dsl::col,
    prelude::{LazyFrame, Schema},
    series::Series,
};

use crate::{
    errors::{UltiResult, UltimaErr},
    filters::{fltr_chain, AndOrFltrChain},
};

use polars::prelude::{DataFrame, IntoLazy};
use serde::{Deserialize, Serialize};

#[cfg(feature = "db")]
pub use self::db::{fltr_chain_to_sql_query, sql_get_column, sql_query, DbInfo};

/// Indicated the source of data
#[derive(Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum SourceVariant {
    #[default]
    InMemory,
    Scan,
    // TODO DB Conn
    #[cfg(feature = "db")]
    Db,
}

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
    pub fn get_lazyframe(&self, filters: &AndOrFltrChain) -> UltiResult<LazyFrame> {
        let filter = fltr_chain(filters);
        match self {
            DataSource::InMemory(df) => {
                if let Some(f) = filter {
                    Ok(df.clone().lazy().filter(f))
                } else {
                    Ok(df.clone().lazy())
                }
            }
            DataSource::Scan(lf) => {
                if let Some(f) = filter {
                    Ok(lf.clone().filter(f))
                } else {
                    Ok(lf.clone())
                }
            }
            // TODO do not unwrap
            #[cfg(feature = "db")]
            DataSource::Db(db) => {
                Ok(sql_query(db, &fltr_chain_to_sql_query(&db.table, filters))?.lazy())
            }
        }
    }
    pub fn get_schema(&self) -> UltiResult<Arc<Schema>> {
        match self {
            DataSource::InMemory(df) => Ok(Arc::new(df.schema())),
            DataSource::Scan(lf) => Ok(lf.schema()?),
            #[cfg(feature = "db")]
            DataSource::Db(db) => Ok(db.schema()),
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
            DataSource::Db(_) => true,
        }
    }

    pub fn get_column(&self, col_name: &str) -> UltiResult<Series> {
        match self {
            DataSource::InMemory(df) => {
                let mut df = df.select([col_name])?;
                let srs = df
                    .pop()
                    .ok_or(UltimaErr::Other(format!("Column {col_name} doesn't exist")))?;
                Ok(srs.unique_stable()?)
            }
            DataSource::Scan(lf) => {
                lf.clone()
                    .select([col(col_name)])
                    .collect()?
                    .pop() //above select guaranteed one column
                    .ok_or(UltimaErr::Other(format!("Column {col_name} doesn't exist")))
            }
            #[cfg(feature = "db")]
            DataSource::Db(db) => sql_get_column(db, col_name),
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

// TODO - consider this:
// /// Trait which defines the behavior of a a Source of Data
// /// examples:
// pub trait DataSource: Send + Sync {
//     fn get_lazyframe(&self, filters: &AndOrFltrChain) -> UltiResult<LazyFrame>;
//     fn get_column(&self, col_name: &str) -> UltiResult<Series> ;
//     fn get_schema(&self) -> UltiResult<Arc<Schema>>;

//     /// InMemory -> false
//     /// Scan -> true
//     /// Db -> true
//     fn prepare_on_each_request(&self) -> bool;
// }

// As per example:
// use std::sync::Arc;

// struct Empty;
// struct Null;

// trait P {
//     fn u(&self) -> u8
//     where Self: Sized {1}
// }

// impl P for Empty{}

// trait GeeksforGeeks{
//     type X;
//     fn gfg_func(&self) -> Self::X;
// }

// impl <U> GeeksforGeeks for U {
//     type X = Arc<dyn P>;
//     fn gfg_func(&self) -> Self::X {
//         Arc::new(Empty{})
//     }
// }

// fn main() {
//     let variable_one = Empty;
//     let variable_two  = Null;

//     let obj: Box<dyn GeeksforGeeks<X=Arc<dyn P>>> = Box::new(variable_two);

//     obj.gfg_func();

// }
