#![allow(clippy::type_complexity)]

pub mod add_row;
pub mod api;
mod datarequest;
pub mod dataset;
mod datasource;
mod filters;
mod measure;
pub mod overrides;
pub mod prelude;

pub use crate::prelude::*;

/// Convert requested measure into [ProcessedMeasure] measure by looking up from all_availiable_measures.
///
/// NOTE: if a measure, which was looked up from all_availiable_measures has a predefined AggExpression
/// then we override requested measure.
///
/// by mapping requested String to a map of all availiable measures
fn measure_builder(
    requested_measures: &[(String, String)],
    all_availiable_measures: &MeasuresMap,
    op: &OCP,
) -> PolarsResult<Vec<ProcessedMeasure>> {
    let res = requested_measures.iter()
        .map(|(measure_name, action)| {

            // Lookup requested measure from all_availiable_measures by name
            let Some(m) = all_availiable_measures.get(measure_name as &str) else {
                return Err(PolarsError::ComputeError(format!("No measure {measure_name} exists for the dataset. Availiable measures are: {:?}",
                    all_availiable_measures.keys()).into()))
            };

            // If measure has predefined aggregation, then override the request
            let action = m.aggregation
                .unwrap_or_else(||action as &str);

            // Lookup action from the list of supported actions
            let Some(act) = api::aggregations::BASE_CALCS.get(action) else {
                return Err(PolarsError::ComputeError(format!("No action {action} supported. Supported actions are: {:?}",
                    api::aggregations::BASE_CALCS.keys()).into()))
            };

            // apply action
            let (calculator, name) = act((m.calculator)(op), measure_name);

            Ok(
                ProcessedMeasure {
                    name,
                    calculator,
                    precomputefilter: m.precomputefilter.clone(),
                }
            )
            }
        )
        .collect::<PolarsResult<Vec<ProcessedMeasure>>>();

    res
}

/// Unlike main Measure struct, this structure holds final name, extended Expr(with aggregation)
/// and the precompute filter.
///
/// This is basically a "processed" measure
struct ProcessedMeasure {
    pub name: String,
    pub calculator: Expr,
    pub precomputefilter: Option<Expr>,
}
