#![allow(clippy::type_complexity)]

mod datarequest;
pub mod dataset;
mod datasource;
mod filters;
mod measure;
pub mod overrides;
pub mod prelude;
pub mod api;

use log::warn;
use polars::prelude::*;

pub use crate::prelude::*;


/// Convert requested measure into actual measure
///
/// by mapping requested String to a map of all availiable measures
fn measure_builder(
    requested_measures: &[(String, String)],
    all_availiable_measures: &MeasuresMap,
    op: &OCP,
) -> Vec<ProcessedMeasure> {
    let mut res = Vec::with_capacity(requested_measures.len());
    for (measure_name, action) in requested_measures {
        if let Some(m) = all_availiable_measures.get(measure_name as &str) {
            if let Some(act) = api::aggregations::BASE_CALCS.get(action as &str) {
                let (expr, newname) = act((m.calculator)(op), measure_name);

                let new_measure = ProcessedMeasure {
                    name: newname,
                    calculator: expr,
                    precomputefilter: m.precomputefilter.clone(),
                };
                res.push(new_measure)
            } else {
                warn!("Aggregation action: {action} not found");
                continue;
            }
        } else {
            warn!("Measure: {measure_name} not found");
            continue;
        }
    }
    res
}

/// Unlike main Measure struct, this structure holds final name, extended Expr and the precompute filter.
///
/// This is basically a "processed" measure
struct ProcessedMeasure {
    pub name: String,
    pub calculator: Expr,
    pub precomputefilter: Option<Expr>,
}
