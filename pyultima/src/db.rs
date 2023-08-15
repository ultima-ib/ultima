use std::sync::Arc;

use polars::prelude::Field;
use pyo3::{pyclass, pymethods};
use ultibi::datasource::DbInfo as DbInfoInner;
use ultibi::polars::prelude::{DataType, Schema};

use crate::conversions::wrappers::Wrap;

/// DataBase Info
/// Needed to determine connection
#[pyclass]
#[derive(Clone)]
pub struct DbInfo {
    #[allow(dead_code)]
    pub inner: DbInfoInner,
}

#[pymethods]
impl DbInfo {
    #[new]
    #[pyo3(signature = (table, db_type, conn_uri, schema))]
    fn __init__(
        table: String,
        db_type: String,
        conn_uri: String,
        schema: Option<Vec<(String, Wrap<DataType>)>>,
    ) -> Self {
        let schema = schema
            .map(|field| {
                Schema::from_iter(
                    field
                        .into_iter()
                        .map(|(name, wrap)| Field::new(name.as_str(), wrap.0)),
                )
            })
            .map(Arc::new);

        DbInfo {
            inner: DbInfoInner {
                table,
                db_type,
                conn_uri,
                schema,
            },
        }
    }

    pub fn __str__(&self) -> String {
        format!("{:?}", self.inner)
    }
}
