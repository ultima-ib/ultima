use serde::{Serialize, Deserialize};
use polars::prelude::*;

use crate::filters::FilterE;

/// DataSet must have column present
/// value must be parsable to the column format (or inner format in case of a list)
/// # Examples
/// ```
/// Json looks like this:
/// {   "column": "SensWeights",
///     "value": "[0.005]",
///     "when": [{"Eq":[["RiskClass", "DRC_NonSec"]]},
///          {"Eq":[["CreditQuality", "AA"]]}]
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Overwrite {
    column: String,
    value: String,
    when: Vec<FilterE>
}

impl Overwrite {
    pub fn override_builder(&self, val: Expr) -> Expr {
        // Empty filter means the whole column will get overwritten
        if self.when.is_empty() {
            return val.alias(&self.column)
        }

        let mut fltr_iter = self.when.iter();
        // First filter
        let mut fltr = fltr_iter.next()
            .unwrap() // form above we know self.when hs at least one element
            .to_expr();
        // All subsequent filters are joined by .and()
        for f in self.when.iter() {
            fltr = fltr.and(f.to_expr())
        }

        when(fltr)
        .then(val)
        .otherwise(col(&self.column))
        .alias(&self.column)

    }

    fn string_to_lit(&self, dt: &DataType) -> PolarsResult<Expr> {
        match dt {
            // RW column is a list for example
            DataType::List(x) => {
                match **x {
                    DataType::Float64 =>{
                        let vc = serde_json::from_str::<Vec<f64>>(&self.value)
                            .map_err(|_|PolarsError::SchemaMisMatch(format!("Argument {} could not be parsed into column {} format. Argument should be a vector",&self.value, &self.column).into()))?;
                            Ok(
                                Expr::Literal(
                                    LiteralValue::try_from(
                                        AnyValue::List(Series::from_vec("NewVal", vc))
                                    )? 
                                ).list() // <-- Needed since this one is a list
                            )
                        },
                    _ => Err(PolarsError::SchemaMisMatch("Only List f64 columns can be overwritten".into())),
                }
            } ,
            // All Numeric columns are f64
            DataType::Float64 => {
                let f = serde_json::from_str::<f64>(&self.value)
                    .map_err(|_|PolarsError::SchemaMisMatch(format!("Argument {} could not be parsed into column {} format. Argument should be a digit",&self.value , &self.column).into()))?;
                    Ok(
                        Expr::Literal(
                            LiteralValue::try_from(
                                AnyValue::Float64(f)
                            )? 
                        )
                    )
                
                },
            // All Other columns are 
            DataType::Utf8 => {
                Ok(
                    Expr::Literal(
                        LiteralValue::try_from(
                            AnyValue::Utf8(&self.value)
                        )? 
                    )
                )
            },
            _ => Err(PolarsError::ComputeError(format!("Column {} of this format cannot be overwritten", &self.column).into())),
        }
    }
    
    pub fn df_with_overwrite(&self, df: DataFrame) -> PolarsResult<DataFrame> {
        let dt = df.column(&self.column)?.dtype();
        let lt = self.string_to_lit(dt)?;
        let new_col_as_expr = self.override_builder(lt);
        df.lazy().with_column(new_col_as_expr).collect()
    }
}
