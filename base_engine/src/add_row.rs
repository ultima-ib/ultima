use std::{collections::HashMap, sync::Arc};

use polars::prelude::{Schema, DataFrame, AnyValue, PolarsResult, row::Row, Field};

use crate::overrides::string_to_any;


/// Convers HashMap into a Frame of particular Schema
/// Filters out any columns not in current schema
pub(crate) fn map_to_row_schema(map: HashMap<String, String>, sch: Arc<Schema>) -> PolarsResult<(Row<'static>, Schema)> {
    let mut vc = Vec::with_capacity(map.len());

    let row = 
        map.iter()
        .filter_map(|(col, val)|{
            if let Some(dt) = sch.get(col) {
                vc.push(Field::new(col, dt.clone()));
                Some(string_to_any(val, dt, &col))
            } else { None }
        })
        .collect::<PolarsResult<Vec<AnyValue>>>()?;

    let schema = Schema::from_iter(vc);
    Ok((Row(row), schema))

    //DataFrame::from_rows_and_schema(&[row], sch.as_ref())
}

pub(crate) fn df_from_maps_and_schema(maps: Vec<HashMap<String, String>>, sch: Arc<Schema>) -> PolarsResult<DataFrame> {
    let rows_schemas = maps.into_iter()
        .map(|map|map_to_row_schema(map, sch.clone()))
        .collect::<PolarsResult<Vec<(Row, Schema)>>>()?;

    let (rows, _schemas): (Vec<Row>, Vec<Schema>) = rows_schemas.into_iter()
        .unzip();
    
    DataFrame::from_rows_and_schema(&rows, sch.as_ref())
}