pub use super::helpers::searches::*;
pub mod execute_agg;
pub use crate::execution::execute_agg::*;
use crate::{errors::UltiResult, ComputeRequest, DataSet};
pub mod execute_agg_with_cache;

/// Distributes work based on request
pub fn execute<DS: DataSet + ?Sized>(
    data: &DS,
    r: ComputeRequest,
    prepare: bool,
) -> UltiResult<DataFrame> {
    match r {
        ComputeRequest::Aggregation(ar) => exec_agg(data, ar, prepare),
        _ => unimplemented!(),
    }
}
