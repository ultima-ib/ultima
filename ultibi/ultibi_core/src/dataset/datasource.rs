use std::sync::Arc;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Schema};
use serde::{Deserialize, Serialize};

use crate::{
    errors::UltiResult,
    filters::{fltr_chain, AndOrFltrChain, FilterE},
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

/// DbInfo Depends on the kind of Db you are connecting to
#[derive(Clone)]
pub struct DbInfo{
    /// Name of the data table
    /// SELECT * FROM table
    pub table: String,

    /// MySQL/Oracle/Postgres etc
    pub db_type: String,

    /// Connection String
    pub conn: String, 
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
            DataSource::Db(_) => true,
        }
    }
}

pub fn fltr_chain_to_sql_query(table: &str, chain: &AndOrFltrChain) -> String {
    
    let mut base = format!("SELECT * FROM {}", table);

    let mut outer = vec![];

    // Loop from outer vec to inner
    for inner_or_filters in chain {
        if inner_or_filters.is_empty() { continue;}
        //let base = if !where_was_added {where_was_added = true; "WHERE "} else {"AND "};

        // To track if we need to prefix OR
        // First iteration is not OR
        let inner_fltrs_sql: Vec<String> = inner_or_filters.iter()
            .map(fltr_to_sql_query)
            .collect();
        let inner_fltrs_sql_joined = inner_fltrs_sql.join(" OR ");

        outer.push(format!("({})", inner_fltrs_sql_joined));
    }

    if !outer.is_empty() {
        let outer_joined = outer.join(" AND ");
        base.push_str(" WHERE ");
        base.push_str(&outer_joined);
    }
    
    base
}

pub fn fltr_to_sql_query(fltr: &FilterE) -> String {
    match fltr {
        FilterE::Eq{field, value} => match value {
            Some(v) => format!("{field} = '{v}'"),
            None => format!("{field} IS NULL")
        },
        FilterE::Neq{field, value} => match value {
            Some(v) => format!("{field} != '{v}'"),
            None => format!("{field} IS NOT NULL")
        },
        FilterE::In{field, value} => vec_to_or_sql(field, value, false),
        FilterE::NotIn{field, value} => vec_to_or_sql(field, value, true)
    }
}

pub fn vec_to_or_sql(field: &str, ors: &[Option<String>], not: bool) -> String {

    let mut has_none = false;
    let mut placeholder = Vec::with_capacity(ors.len());
    ors.iter()
        .for_each(|x|
            if let Some(x) = x {
                placeholder.push(format!("'{}'", x));
            } 
            else {has_none=true}
        );
    
    let not = if not {"NOT"} else {""};
    let mut query = format!("{field} {not} IN ({})", placeholder.join(","));
    if has_none {
        
        query.push_str(&format!(" OR {field} IS {not} NULL"));
    }
    query

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



/// Helper. A wrapper around DataSource
#[derive(Clone)]
pub struct DataSourceBase<S: DataSourceT>(pub S);

/// A DataSource must provide these fields at least
pub trait DataSourceT {
    /// Return LazyFrame to accommodate for both Scan and InMemory cases
    fn get_lazyframe(&self, filters: &AndOrFltrChain) -> LazyFrame;
    fn get_schema(&self) -> UltiResult<Arc<Schema>>;
    fn prepare_on_each_request(&self) -> bool;
}
