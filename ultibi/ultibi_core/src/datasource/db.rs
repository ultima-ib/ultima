use std::sync::Arc;

use super::db_utils::record_batches_to_df;
use super::DataSource;
use crate::errors::UltiResult;
use crate::{
    errors::UltimaErr,
    filters::{AndOrFltrChain, FilterE},
};
use connectorx::prelude::get_arrow;
use connectorx::{source_router::SourceConn, sql::CXQuery};
use polars::frame::DataFrame;
use polars::{
    lazy::dsl::col,
    prelude::{IntoLazy, Schema},
    series::Series,
};

/// DbInfo Depends on the kind of Db you are connecting to
#[derive(Clone, Debug)]
pub struct DbInfo {
    /// Name of the data table
    /// SELECT * FROM table
    pub table: String,

    /// MySQL/Oracle/Postgres etc
    pub db_type: String,

    /// Used to determine the appropriate DataType for each column
    /// If a column received from DB is not in schema, we assume it's text
    pub schema: Arc<Schema>,

    /// Connection String
    pub conn_uri: String,
}

impl DbInfo {
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

pub fn sql_get_column(db: &DbInfo, col_name: &str) -> UltiResult<Series> {
    let query = format!("SELECT DISTINCT {} FROM {}", col_name, db.table);

    let db = sql_query(db, &query)?;

    let srs = db.column(col_name)?.clone();

    Ok(srs)
}

pub fn sql_query(db: &DbInfo, query: &str) -> UltiResult<DataFrame> {
    let source_conn = SourceConn::try_from(db.conn_uri.as_str())
        .map_err(|err| UltimaErr::Other(err.to_string()))?;

    let queries = &[CXQuery::from(query)];

    let destination =
        get_arrow(&source_conn, None, queries).map_err(|err| UltimaErr::Other(err.to_string()))?;

    let data = destination
        .arrow()
        .map_err(|err| UltimaErr::Other(err.to_string()))?;

    let df = record_batches_to_df(data)?;

    // We need to perform some casting
    let mut casts = vec![];
    // First, into the expected schema if that was provided
    db.schema
        .iter_fields()
        .for_each(|f| casts.push(col(f.name()).cast(f.data_type().clone())));

    // Assume query always returns all columns of schema
    df.lazy()
        .with_columns(casts)
        .collect()
        .map_err(UltimaErr::Polars)
}

pub fn fltr_chain_to_sql_query(table: &str, chain: &AndOrFltrChain) -> String {
    let mut base = format!("SELECT * FROM {}", table);

    let mut outer = vec![];

    // Loop from outer vec to inner
    for inner_or_filters in chain {
        if inner_or_filters.is_empty() {
            continue;
        }

        // To track if we need to prefix OR
        // First iteration is not OR
        let inner_fltrs_sql: Vec<String> = inner_or_filters.iter().map(fltr_to_sql_query).collect();
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
        FilterE::Eq { field, value } => match value {
            Some(v) => format!("({field} = '{v}')"),
            None => format!("({field} IS NULL)"),
        },
        FilterE::Neq { field, value } => match value {
            Some(v) => format!("({field} != '{v}' OR {field} IS NULL)"),
            None => format!("({field} IS NOT NULL)"),
        },
        FilterE::In { field, value } => format!("({})", vec_to_or_sql(field, value, false)),
        FilterE::NotIn { field, value } => format!("({})", vec_to_or_sql(field, value, true)),
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
pub fn vec_to_or_sql(field: &str, ors: &[Option<String>], not: bool) -> String {
    let mut has_none = false;
    let mut placeholder = Vec::with_capacity(ors.len());

    ors.iter().for_each(|x| {
        if let Some(x) = x {
            if !not {
                placeholder.push(format!("{field} = '{x}'"));
            } else {
                placeholder.push(format!("{field} != '{x}'"));
            }
        } else {
            has_none = true
        }
    });

    let joiner = if not { " AND " } else { " OR " };
    let mut query = placeholder.join(joiner);

    // Special cases - check docs
    if (not & (!has_none)) | ((!not) & has_none) {
        query.push_str(&format!(" OR {field} IS NULL"));
    }
    query
}

impl From<DbInfo> for DataSource {
    fn from(item: DbInfo) -> Self {
        DataSource::Db(item)
    }
}
