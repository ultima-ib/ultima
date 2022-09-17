#![allow(clippy::type_complexity)]

mod datarequest;
pub mod dataset;
mod datasource;
mod filters;
mod measure;
pub mod overrides;
pub mod prelude;

use log::warn;
use polars::prelude::*;

use once_cell::sync::Lazy;
use std::collections::HashMap;

pub use crate::prelude::*;

/// main function which returns a Result of the calculation
/// currently support only the first element of frames
pub fn execute(req: AggregationRequest, data: &impl DataSet) -> Result<DataFrame> {
    // Assuming Front End knows which columns can be in groupby, agg etc

    // Step 0.1
    let f1 = &data.frames()[0];
    //let tmp = f1.clone().lazy().filter(col("RiskClass").eq(lit("DRC_SecNonCTP"))).collect()?;
    //dbg!(&tmp["SensWeights"]);
    //let f1_cols = f1.get_column_names();

    // Polars DataFrame clone is cheap:
    // https://stackoverflow.com/questions/72320911/how-to-avoid-deep-copy-when-using-groupby-in-polars-rust
    let mut f1 = f1.clone().lazy();
    let measure_map = data.measures();

    // Step 1.0 Applying FILTERS:
    // TODO check if column is present in DF - ( is this "second line of defence" even needed?) 
    for f in req.filters() {
        f1 = f1.filter(f.to_expr());
    }

    // Step 2.x OVERRIDE, 2.1 GROUPBY, 2.2 Aggregate and Calculate Measures

    // Step 2.1 Build AGGREGATIONS/Measures
    // First, parse requested measures
    let m = req.measures();
    let op = req.calc_params();

    let prepared_measures = measure_builder(m, measure_map, op);

    // Unpack - (New Column Name, AggExpr, MeasureSpecificFilter)
    let (newnames, (aggregateions, fltrs)): (Vec<String>, (Vec<Expr>, Vec<Option<Expr>>)) =
        prepared_measures
            .into_iter()
            .map(|m| (m.name, (m.calculator, m.precomputefilter)))
            .unzip();

    // Step 2.2 Build Measure Specific Filter
    // Note: DOESN'T WORK .or(lit::<bool>(true))
    // By default, everything is false (ie everything is filtered out)
    let mut measure_filter_opt = Some(lit::<bool>(false));
    for fltr in fltrs {
        match fltr {
            // join filters as or
            Some(f) => {
                measure_filter_opt = measure_filter_opt.map(|fltr|fltr.or(f)); //= measure_filter_opt.or(f);
            }
            // If at least one of the measure filters is None, then everything is true
            // and break
            None => {
                measure_filter_opt = None;
                break
            }
        }
    }

    
    
    // Step 2.3 Applying (Mesure)FILTER
    if let Some(fltr) = measure_filter_opt{
        f1 = f1.filter(fltr)
    }
    
    // Step 2.4 Applying Overwrites
    let mut df = f1.collect()?;
    for ow in req.overrides() {
        df = ow.df_with_overwrite(df)?
    }
    
    
    // Step 2.4 Build GROUPBY
    let groups: Vec<Expr> = req._groupby().iter().map(|x| col(x)).collect();

    // Step 2.5 Apply GroupBy and Agg
    // Note .limit doesn't work with standard groupby on large frames
    // hence use groupby_stable
    f1 = df.lazy().groupby_stable(groups)
        .agg(aggregateions)
        .limit(1_000);

    // POSTPROCESSING
    // Remove zeros, optional
    // TODO Note: Comparing with 0. doesn't work with list columns
    // TODO Need to check column type and based on that compare against 0. or not
    if req.hide_zeros
    {
        let mut it = newnames.iter();
        if let Some(c) = it.next() {
            // Filter where col is Not Eq 0 AND Not Eq Null
            let mut predicate = col(c).neq(lit::<f64>(0.)).and(col(c).neq(NULL.lit()));
            for c in it {
                predicate = predicate.or(col(c).neq(lit::<f64>(0.)).and(col(c).neq(NULL.lit())))
            }
            f1 = f1.filter(predicate);
        }
    };
    
    f1.collect()
}

/// Convert requested measure into actual measure
///
/// by mapping requested String to a map of all availiable measures
fn measure_builder(
    requested_measures: &[(String, String)],
    all_availiable_measures: &MM,
    op: &OCP,
) -> Vec<ProcessedMeasure> {
    let mut res = Vec::with_capacity(requested_measures.len());
    for (measure_name, action) in requested_measures {
        if let Some(m) = all_availiable_measures.get(measure_name as &str) {
            if let Some(act) = BASE_CALCS.get(action as &str) {
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

/// Unlike main Measure struct, this structure holds final name, evaluated Expr and the precompute filter.
///
/// This is basically a "processed" measure
struct ProcessedMeasure {
    pub name: String,
    pub calculator: Expr,
    pub precomputefilter: Option<Expr>,
}

/// This static exists because we need to map aggregation request to a function
/// There doesn't seem to be a better way than keeping a HashMap
/// TODO AggExpr implements serde ser/deser
/// hence we can do smth like serde_json::from_str<AggExpr>()
pub static BASE_CALCS: Lazy<HashMap<&'static str, fn(Expr, &str) -> (Expr, String)>> =
    Lazy::new(|| {
        HashMap::from([
            ("sum", sum as fn(Expr, &str) -> (Expr, String)),
            ("min", min),
            ("max", max),
            ("mean", mean),
            ("var", var),
            ("quantile95low", quantile_95_lower),
            ("first", first),
            //("list", list), <-> not needed
            ("count", count),
            ("count_unique", count_unique),
        ])
    });

fn sum(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_sum");
    (c.sum().alias(alias.as_ref()), alias)
}
fn min(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_min");
    (c.min().alias(alias.as_ref()), alias)
}
fn max(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_max");
    (c.max().alias(alias.as_ref()), alias)
}
fn mean(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_mean");
    (c.mean().alias(alias.as_ref()), alias)
}
fn var(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_var");
    (c.var().alias(alias.as_ref()), alias)
}
fn quantile_95_lower(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_quantile95lower");
    (
        c.quantile(0.95, QuantileInterpolOptions::Lower)
            .alias(alias.as_ref()),
        alias,
    )
}
/// Not including "_first" alias to avoid confusion
/// First is usually used by measures such as Capital or RiskWeight
/// Which are calculated at a level of a certain column such as RiskFactor
fn first(c: Expr, newname: &str) -> (Expr, String) {
    (c.first().alias(newname), newname.to_string())
}
//fn list(c: Expr, newname: &str) -> (Expr, String) {
//    let alias = format!("{newname}_list");
//    ( c.list().alias(alias.as_ref()), alias)
//}
fn count(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    (c.count().alias(alias.as_ref()), alias)
}
fn count_unique(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    (c.n_unique().alias(alias.as_ref()), alias)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
