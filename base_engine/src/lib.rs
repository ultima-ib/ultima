#![allow(clippy::type_complexity)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![doc(html_no_source)]

pub mod add_row;
pub mod api;
mod datarequest;
pub mod dataset;
mod datasource;
mod filters;
mod measure;
pub mod overrides;
pub mod prelude;

use crate::polars::error::PolarsError;

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
        .map(|(requested_measure, requested_action)| {

            // Lookup requested measure from all_availiable_measures by name
            let Some(m) = all_availiable_measures.get(requested_measure as &str) else {
                return Err(PolarsError::ComputeError(format!("No measure {requested_measure} exists for the dataset. Availiable measures are: {:?}",
                    all_availiable_measures.keys()).into()))
            };

            // If measure has predefined aggregation, check that requested aggregation matches it          
            if let Some(default_action) = m.aggregation {
                if default_action != requested_action {
                    return Err(PolarsError::ComputeError(format!("Measure {requested_measure} supports only {default_action} aggregation,
                    but {requested_action} requested").into()))
                }
            }

            // Lookup action from the list of supported actions
            let Some(act) = api::aggregations::BASE_CALCS.get(requested_action.as_str()) else {
                return Err(PolarsError::ComputeError(format!("No action {requested_action} supported. Supported actions are: {:?}",
                    api::aggregations::BASE_CALCS.keys()).into()))
            };

            // apply action
            let (calculator, name) = act((m.calculator)(op), requested_measure);

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
