use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::filters::{fltr_chain, AndOrFltrChain};

/// DataSet must have column present
/// value must be parsable to the column format (or inner format in case of a list)
/// # Examples
/// ```
/// /*
/// Json looks like this:
/// {   "field": "SensWeights",
///     "value": "[0.005]",
///     "filters": []
/// }
/// */
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Override {
    field: String,
    value: String,
    filters: AndOrFltrChain,
}

impl Override {
    pub fn override_builder(&self, val: Expr) -> Expr {
        // Empty filter means the whole column will get overwritten
        let fltr = fltr_chain(&self.filters);
        // if filter was provided
        if let Some(f) = fltr {
            when(f)
                .then(val)
                .otherwise(col(&self.field))
                .alias(&self.field)
        } else {
            // otherwise we simply override the whole column
            val.alias(&self.field)
        }
    }

    pub fn lf_with_overwrite(&self, lf: LazyFrame) -> PolarsResult<LazyFrame> {
        let schema = lf.schema()?;
        let dt = schema.try_get(self.field.as_str())?;

        let mut lt = Expr::Literal(LiteralValue::try_from(string_to_any(
            &self.value,
            dt,
            &self.field,
        )?)?);

        if let DataType::List(_) = dt {
            lt = lt.implode()
        }
        let new_col_as_expr = self.override_builder(lt);
        Ok(lf.with_column(new_col_as_expr))
    }
}

/// This function also defines Column DataTypes which we can override  
pub(crate) fn string_to_any<'a>(
    value: &'a str,
    dt: &DataType,
    column_name: &str,
) -> PolarsResult<AnyValue<'a>> {
    let emsg = format!(
        "Argument {value} could not be parsed into column {column_name} format. Argument should be a {dt}",
    );

    match dt {
        // RW column is a list for example
        DataType::List(x) => match **x {
            DataType::Float64 => serde_json::from_str::<Vec<f64>>(value)
                .map_err(|_| PolarsError::SchemaMismatch(emsg.into()))
                .map(|vc| AnyValue::List(Series::from_vec("NewVal", vc))),
            _ => Err(PolarsError::SchemaMismatch(
                "Only List f64 columns can be overwritten".into(),
            )),
        },
        // All Numeric columns are f64
        DataType::Float64 => {
            let f = serde_json::from_str::<f64>(value)
                .map_err(|_| PolarsError::SchemaMismatch(emsg.into()))?;
            Ok(AnyValue::Float64(f))
        }
        // Python int
        DataType::Int64 => {
            let i = serde_json::from_str::<i64>(value)
                .map_err(|_| PolarsError::SchemaMismatch(emsg.into()))?;
            Ok(AnyValue::Int64(i))
        }
        // Boolean column
        DataType::Boolean => Ok(AnyValue::Boolean(
            serde_json::from_str::<bool>(value)
                .map_err(|_| PolarsError::SchemaMismatch(emsg.into()))?,
        )),
        // All Other columns are
        DataType::Utf8 => Ok(AnyValue::Utf8(value)),

        _ => Err(PolarsError::ComputeError(
            format!("Column {column_name} of this format cannot be overwritten",).into(),
        )),
    }
}
