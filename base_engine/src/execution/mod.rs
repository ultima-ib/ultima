pub use super::helpers::searches::*;
pub mod execute_agg;
use crate::{DataSet, ComputeRequest};
pub use crate::execution::execute_agg::*;
pub mod execute_agg_with_cache;

/// Distributes work based on request
pub fn execute<DS: DataSet + ?Sized>(data: &DS, r: ComputeRequest, streaming: bool) -> PolarsResult<DataFrame> {
    match r {
        ComputeRequest::Aggregation(ar) => {
            exec_agg(data, ar, streaming)
        }
        _ => unimplemented!()
    }
}
