use std::collections::BTreeMap;
use std::sync::Arc;

use polars::frame::row::Row;
use polars::functions::diag_concat_df;
use polars::prelude::{AnyValue, DataFrame, Field, PolarsResult, Schema};
use serde::{Deserialize, Serialize};

use crate::overrides::string_to_any;

/// wrapper for Additional Rows used in [AggregationRequest]
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdditionalRows {
    /// Flag to indicate if .prepare() should be called or not
    /// eg Assign Weights or not?
    /// If Assign Weights than make sure alll the required columns are present
    pub prepare: bool,
    /// new rows {colName: colValue}
    pub rows: Vec<BTreeMap<String, String>>,
}
/// Convers HashMap into a Frame of particular Schema
/// Filters out any columns not in current schema
pub(crate) fn map_to_row_schema(
    map: &BTreeMap<String, String>,
    sch: Arc<Schema>,
) -> PolarsResult<(Row, Schema)> {
    let mut vc = Vec::with_capacity(map.len());

    let row = map
        .iter()
        .filter_map(|(col, val)| {
            if let Some(dt) = sch.get(col) {
                vc.push(Field::new(col, dt.clone()));
                Some(string_to_any(val, dt, col))
            } else {
                None
            }
        })
        .collect::<PolarsResult<Vec<AnyValue>>>()?;

    let schema = Schema::from_iter(vc);
    Ok((Row(row), schema))
}

/// Creates a frame from each Schema.
/// This allows to combine different schemas within add_row part of request.
/// Diagonally concatenates these.
pub(crate) fn df_from_maps_and_schema(
    maps: &[BTreeMap<String, String>],
    sch: Arc<Schema>,
) -> PolarsResult<DataFrame> {
    let new_rows = maps
        .iter()
        .map(|map| {
            map_to_row_schema(map, sch.clone())
                .and_then(|(r, s)| DataFrame::from_rows_and_schema(&[r], &s))
        })
        .collect::<PolarsResult<Vec<DataFrame>>>()?;

    diag_concat_df(&new_rows)
}
