use std::sync::Arc;

#[cfg(feature = "db")]
use connectorx::{sql::CXQuery, source_router::SourceConn};

use polars::{prelude::{DataFrame, IntoLazy, LazyFrame, Schema, DataType}, lazy::dsl::col};
use serde::{Deserialize, Serialize};

#[cfg(feature = "db")]
use connectorx::prelude::get_arrow2;

use crate::{
    errors::{UltiResult, UltimaErr},
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

    /// Used to determine the appropriate DataType for each column
    /// If None, we will use what we get back from SQL Server
    pub schema: Option<Arc<Schema>>,

    /// Connection String
    pub conn_uri: String, 
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
            // TODO do not unwrap
            #[cfg(feature = "db")]
            DataSource::Db(db) => sql_query(db, &fltr_chain_to_sql_query(&db.table, filters)).unwrap(),
        }
    }
    pub fn get_schema(&self) -> UltiResult<Arc<Schema>> {
        match self {
            DataSource::InMemory(df) => Ok(Arc::new(df.schema())),
            DataSource::Scan(lf) => Ok(lf.schema()?),
            #[cfg(feature = "db")]
            DataSource::Db(db) => sql_schema(db),
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

#[cfg(feature = "db")]
pub fn sql_schema(db: &DbInfo) -> UltiResult<Arc<Schema>> {
    if let Some(schema) = &db.schema {
        Ok(Arc::clone(schema))
    } else {
        // TODO would have to match based on db type
        let query = format!("SELECT * FROM delta {} LIMIT 100", db.table);
        let schema = sql_query(db, &query)?
        .schema()?;
        dbg!(schema.as_ref());
        Ok(schema)
    }
}

#[cfg(feature = "db")]
pub fn sql_query(db: &DbInfo, query: &str) -> UltiResult<LazyFrame> {

    let source_conn = SourceConn::try_from(db.conn_uri.as_str())
        .map_err(|err|UltimaErr::Other(err.to_string()))?;

    let queries = &[CXQuery::from(query)];

    let destination = get_arrow2(&source_conn, None, queries)
        .map_err(|err|UltimaErr::Other(err.to_string()))?;

    let data = destination.polars()
        .map_err(|err|UltimaErr::Other(err.to_string()))?;

    // We need to perform some casting
    let mut casts = vec![];

    // First, into the expected schema if that was provided
    if let Some(sch) = &db.schema {
        sch.iter_fields()
        .for_each(|f| casts.push(col(f.name()).cast(f.data_type().clone())));    
    } else { // if wasn't provided we simply do the
    // workaround for https://github.com/sfu-db/connector-x/issues/510
    let schema = data.schema();
    schema.iter_fields()
        .filter(|field|matches!(
            field.data_type(),
            DataType::Binary))
        .for_each(|f| casts.push(col(f.name()).cast(DataType::Utf8)));
    }

    Ok( data.lazy().with_columns(casts).collect()?.lazy() )

}

#[cfg(feature = "db")]
pub fn fltr_chain_to_sql_query(table: &str, chain: &AndOrFltrChain) -> String {

    let mut base = format!("SELECT * FROM {}", table);

    let mut outer = vec![];

    // Loop from outer vec to inner
    for inner_or_filters in chain {
        if inner_or_filters.is_empty() { continue;}

        // To track if we need to prefix OR
        // First iteration is not OR
        let inner_fltrs_sql: Vec<String> = inner_or_filters.iter()
            .map(fltr_to_sql_query)
            .collect();
        let inner_fltrs_sql_joined = inner_fltrs_sql.join(" OR ");

        outer.push(format!("({})", inner_fltrs_sql_joined));
    }

    if !outer.is_empty() {
        base.push_str(" WHERE ");

        let outer_joined = outer.join(" AND ");
        base.push_str(&outer_joined);
    }
    
    base
}

#[cfg(feature = "db")]
pub fn fltr_to_sql_query(fltr: &FilterE) -> String {
    match fltr {
        FilterE::Eq{field, value} => match value {
            Some(v) => format!("({field} = '{v}')"),
            None => format!("({field} IS NULL)")
        },
        FilterE::Neq{field, value} => match value {
            Some(v) => format!("({field} != '{v}' OR {field} IS NULL)"),
            None => format!("({field} IS NOT NULL)")
        },
        FilterE::In{field, value} => format!("({})", vec_to_or_sql(field, value, false)),
        FilterE::NotIn{field, value} => format!("({})", vec_to_or_sql(field, value, true))
    }
}

/// SELECT * FROM delta
/// -- vec![FilterE::Eq{field:"RiskCategory".into(), value: Some("Delta".into()) }, FilterE::Neq{field:"RiskCategory".into(), value: Some("Vega".into()) }],
/// WHERE ((RiskCategory = 'Delta') OR (RiskCategory != 'Vega'))
/// -- vec![FilterE::In{field:"RiskClass".into(), value: vec![Some("FX".into()), Some("Commodity".into()), None]}],
/// AND ((RiskClass = 'FX' OR RiskClass = 'Commodity' OR RiskClass IS NULL))
/// -- vec![FilterE::NotIn{field:"CommodityLocation".into(), value: vec![Some("London".into()), Some("China".into())]}],
/// -- when Not and no NULL must add OR . IS NULL
/// AND ((CommodityLocation != 'China' AND CommodityLocation != 'NY' OR CommodityLocation IS NULL))
/// -- unless notEq contains NULL
/// -- vec![FilterE::NotIn{field:"RiskFactor".into(), value: vec![Some("EURUSD".into()), Some("GBPEUR".into()), None]}],
/// -- then we leave it out, since SQL filter out NULLs on != anyway
/// AND ((RiskFactor != 'EURUSD' AND RiskFactor != 'GBPEUR'));
/// 
/// For more info see tests
#[cfg(feature = "db")]
pub fn vec_to_or_sql(field: &str, ors: &[Option<String>], not: bool) -> String {

    let mut has_none = false;
    let mut placeholder = Vec::with_capacity(ors.len());

    ors.iter()
        .for_each(|x|
            if let Some(x) = x {
                if !not {
                    placeholder.push(format!("{field} = '{x}'"));
                } else {
                    placeholder.push(format!("{field} != '{x}'"));
                }  
            } 
            else {has_none=true}
        );
    
    let joiner = if not {" AND "} else {" OR "};
    let mut query = placeholder.join(joiner);

    // Special cases - check docs
    if (not&(!has_none)) | ((!not)&has_none) {
        query.push_str(&format!(" OR {field} IS NULL"));
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



// TODO
// #[derive(Clone)]
// pub struct DataSourceBase<S: DataSourceT>(pub S);

// /// A DataSource must provide these fields at least
// pub trait DataSourceT {
//     /// Return LazyFrame to accommodate for both Scan and InMemory cases
//     fn get_lazyframe(&self, filters: &AndOrFltrChain) -> LazyFrame;
//     fn get_schema(&self) -> UltiResult<Arc<Schema>>;
//     fn prepare_on_each_request(&self) -> bool;
// }
