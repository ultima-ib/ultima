use std::{collections::HashMap, sync::Arc};

use polars::prelude::{Schema, DataFrame, AnyValue, PolarsResult, row::Row};

use crate::overrides::string_to_any;


/// Convers HashMap into a Frame of particular Schema
/// If 
pub(crate) fn map_to_row(map: HashMap<String, String>, sch: Arc<Schema>) -> PolarsResult<Row<'static>> {

    let row = 
        sch.iter()
        .map(|(col, dt)|{
            if let Some(val) = map.get(col) {
                string_to_any(val, dt, &col)
            } else {
                Ok(AnyValue::Null)
            }
        })
        .collect::<PolarsResult<Vec<AnyValue>>>()?;

    Ok(Row(row))

    //DataFrame::from_rows_and_schema(&[row], sch.as_ref())
}

pub(crate) fn df_from_maps_and_schema(maps: Vec<HashMap<String, String>>, sch: Arc<Schema>) -> PolarsResult<DataFrame> {
    let rows = maps.into_iter()
        .map(|map|map_to_row(map, sch.clone()))
        .collect::<PolarsResult<Vec<Row>>>()?;
    
    DataFrame::from_rows_and_schema(&rows, sch.as_ref())
}