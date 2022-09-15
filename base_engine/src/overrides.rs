use serde::{Serialize, Deserialize};

use crate::filters::FilterE;

/// DataSet must have column present
/// value must be parsable to the column format (or inner format in case of a list)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Override {
    column: String,
    value: String,
    filters: Vec<FilterE>
}