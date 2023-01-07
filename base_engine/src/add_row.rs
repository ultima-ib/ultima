use std::{collections::HashMap, sync::Arc};

use polars::functions::diag_concat_df;
use polars::prelude::{row::Row, AnyValue, DataFrame, Field, PolarsResult, Schema};

use crate::overrides::string_to_any;

/// Convers HashMap into a Frame of particular Schema
/// Filters out any columns not in current schema
pub(crate) fn map_to_row_schema(
    map: &HashMap<String, String>,
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
    maps: Vec<HashMap<String, String>>,
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
