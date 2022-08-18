pub mod prelude;
mod filters;
pub mod dataset;
mod datarequest;
mod datasource;
mod measure;

use polars::prelude::*;
use log::warn;

use once_cell::sync::Lazy;
use std::collections::HashMap;

pub use crate::prelude::*;
use crate::filters::*;
//use crate::dataset::*;
//use crate::datarequest::*;

/// main function which returns a Result of the calculation
/// currently support only the first element of frames
pub fn execute(req: DataRequestS, data: &impl DataSet)
 -> Result<DataFrame>{ 
    // Assuming Front End knows which columns can be in groupby, agg etc

    //info!("f1 with HMS, attr and prepared: {:?}", data.frames()[0].clone().lazy().collect());

    // Step 0.1
    let f1 = &data.frames()[0];
    let f1_cols = f1.get_column_names();
    let mut f1 = f1.clone().lazy();
    let measure_map = data.measures();
    
     

    //Step 1.0 Applying FILTERS:
    for f in req.filters() {
        match f {

            FilterS::In(ref fltrs) => {
                if f1_cols.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_in_or_builder(fltrs));
                };
            },

            FilterS::NotIn(ref fltrs) => {
                if f1_cols.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_not_in_or_builder(fltrs));
                };
            },

            FilterS::Eq(ref fltrs) => {
                if f1_cols.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_eq_or_builder(fltrs));
                };
            } ,

            FilterS::Neq(ref fltrs) => {
                if f1_cols.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_neq_or_builder(fltrs));
                };
            },
        }
    }
    //TODO Step 2.0 Check that unique count based on groups is not larger than the MAX number
    //This helps avoid problems like groupby TradeId
    //let check_df = f1.clone().unique(Some(req._groupby().to_owned()), UniqueKeepStrategy::First).collect()?;
    //if check_df.height() > 100 {
    //    return Err(PolarsError::InvalidOperation("Requested operation results in over 100 groups. Please make sure number of groups does not exceed 100".into()))
    //}
    //This is expensive and slow. Feature raised:
    //https://github.com/pola-rs/polars/issues/4305

    

    // Step 2.1 GROUPBY and 2.2 Calculate Measures

    //AGGREGATE
    //Potentially rayon spawn here, for each measure-df
    //ie we can do all the delta || to vega || to curv
    //the join on groupby cols
    //use https://doc.rust-lang.org/std/panic/fn.catch_unwind.html
    let m = req.measures();
    let op = req.optiona_params();
    let calc_p: &OCP = match op.as_ref(){
        Some(x) => &x.calc_params,
        _ => &None,
    };
    // Build AGG
    let (aggregateions, newnames) = agg_builder(m, measure_map, &calc_p);
    
    //Build GROUPBY
    let groups: Vec<Expr> = req._groupby()
        .iter()
        .map(|x| { col(x) })
        .collect();

    // GROUPBY and AGGREGATE
    // Note .limit doesn't work with standard groupby on large frames
    // hence use groupby_stable
    f1 = f1.groupby_stable(groups)
        .agg(aggregateions)
        .limit(1000);

    // POSTPROCESSING
    // Remove zeros, optional
    // TODO Note: Comparing with 0. doesn't work with list columns
    // TODO Need to check column type and based on that compare against 0. or not
    if req.optiona_params().as_ref()
        .and_then(|x|Some(x.hide_zeros))
        .unwrap_or_default() {
            let mut it = newnames.iter();
            if let Some(c) = it.next() {
                // Filter where col is Not Eq 0 AND Not Eq Null
                let mut predicate = col(c).neq(lit::<f64>(0.)) .and(col(c).neq(NULL.lit()));
                for c in  it {
                    predicate = predicate.or(col(c).neq(lit::<f64>(0.)). and(col(c).neq(NULL.lit())))
                }
                f1 = f1.filter(predicate);
            }
        };
    // Fetch limits the number of computed groups
    // required to make sure server does hang up
    f1.collect()
 }



/// This fn to be called per DataFrame
/// Builds aggregation expression + the new name
/// If requested measure does not exist in all_measures_map it will be simply skipped
fn agg_builder(measures_requested: &[(String, String)], 
    all_measures_map: &MM,
    op: &OCP ) -> (Vec<Expr>, Vec<String>) {
    let mut res_exprs: Vec<Expr> = Vec::with_capacity(measures_requested.len());
    let mut res_alias: Vec<String> = Vec::with_capacity(measures_requested.len());
    for (measure_name, action) in measures_requested {
        let column: Expr;
        
        // First, get the calculator for {measure_name} from {all_measures_map}
        match all_measures_map.get(measure_name as &str) {
            Some(m) => column = (m.calculator)(op),
            _ => {warn!("Measure: {measure_name} not found"); continue }
        }

        // Finally, apply correct aggregation to the measure (eg sum/first etc)
        match BASE_CALCS.get(action as &str) {
            Some(act) =>{
                let (expr, newname) = act(column, measure_name);
                res_exprs.push(expr);
                res_alias.push(newname);
            },
            _ => {warn!("Aggregation action: {action} not found"); continue }
        }
        }
    (res_exprs, res_alias)
}



/// This static exists because we need to map aggregation request to a function
/// There doesn't seem to be a better way than keeping a HashMap
pub static BASE_CALCS: Lazy<HashMap<&'static str, fn(Expr, &str) -> (Expr, String)>> = Lazy::new( || { HashMap::from(
    [
    ("sum", sum as fn(Expr, &str) -> (Expr, String)),
    ("min", min),
    ("max", max),
    ("mean", mean),
    ("var", var),
    ("quantile95low", quantile_95_lower),
    ("first", first),
    //("list", list), <-> not needed
    ("count", count),
    ("count_unique", count_unique)
    ]
    )
});

fn sum(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_sum");
    ( c.sum().alias(alias.as_ref()), alias )
}
fn min(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_min");
    ( c.min().alias(alias.as_ref()), alias )
}
fn max(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_max");
    ( c.max().alias(alias.as_ref()), alias )
}
fn mean(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_mean");
    ( c.mean().alias(alias.as_ref()), alias )
}
fn var(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_var");
    ( c.var().alias(alias.as_ref()), alias)
}
fn quantile_95_lower(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_quantile95lower");
    ( c.quantile(0.95, QuantileInterpolOptions::Lower).alias(alias.as_ref()), alias )
}
/// Not including "_first" alias to avoid confusion 
/// First is usually used by measures such as Capital or RiskWeight
/// Which are calculated at a level of a certain column such as RiskFactor
fn first(c: Expr, newname: &str) -> (Expr, String) { 
    ( c.first().alias(newname), newname.to_string() )
}
//fn list(c: Expr, newname: &str) -> (Expr, String) {
//    let alias = format!("{newname}_list");
//    ( c.list().alias(alias.as_ref()), alias)
//}
fn count(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    ( c.count().alias(alias.as_ref()), alias)
}
fn count_unique(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    ( c.n_unique().alias(alias.as_ref()), alias)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}