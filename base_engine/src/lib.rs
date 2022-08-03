pub mod prelude;
mod filters;
mod dataset;
mod datarequest;
mod datasource;
mod measure;

use polars::prelude::*;
use log::{warn, debug, info};
use lazy_static::lazy_static;
use std::collections::HashMap;

use crate::prelude::*;
use crate::filters::*;
//use crate::dataset::*;
//use crate::datarequest::*;

/// main function which returns a Result DataFrame
pub fn execute(req: DataRequestS, data: &DataSet, measure_map: Arc<MM>)
 -> Result<DataFrame>{ 
    // Step 0 Validate Filters - server to hold list of params availiable as 
    // measures(agg), agg, group, filters
    // All these would be based on columns of DataSet. Measures would be most challenging
    // Recall within same Filter columns should be from the same file

    info!("f1 with HMS, attr and prepared: {:?}", 
    data.f1.clone().lazy().collect());

    // Step 1.0 SELECT columns required for current request
    // In case of SQL data source we need to create DF from 
    // This way we minimize total cloning by selecting only relevant columns
    let (f1_cols, f2_cols, f3_cols) = (
        data.f1.get_column_names(),
        data.f2.get_column_names(),
        data.f3.get_column_names()  
        );
    
    // No need to select since DataFrame clones are super cheap
    //let mut f1_select: Vec<&str> = vec![]; 
    //let mut f2_select: Vec<&str> = vec![]; 
    //let mut f3_select: Vec<&str> = vec![];
    let f1_select = f1_cols;

    /*
    //let (req_cols, bespoke_m) = req.required_columns(measure_col);
    //debug!("Required columns: {:?}", req_cols);
    for col in req_cols.iter() {
        let mut present = false;

        if f1_cols.contains(&col.as_str()) {
            f1_select.push(col);
            present = true;
        } ;

        if f2_cols.contains(&col.as_str()) {
            f2_select.push(col);
            present = true;
        } ;
        
        if f3_cols.contains(&col.as_str()) {
            f3_select.push(col);
            present = true;
        } ;

        // this is additional level of check, making sure each of required cols
        // is present in the DataSet.
        //if not we should skip measure
        if !present {
            warn!("Column {} must be present due to Request selection.", col)
            // return Anyhow error here(converts into polarsError) 
        }
    };
    */
    
    //this clones (but only selected) part of the Frame
    //let mut f1 = data.f1.select(f1_select.clone())?.lazy();
    //let mut f2 = data.f2.select(f2_select)?.lazy();
    //let mut f3 = data.f3.select(f3_select)?.lazy();
    let mut f1 = data.f1().lazy();

    //Step 1.1 Applying FILTERS:

    for f in req.filters() {
        match f {

            FilterS::In(ref fltrs) => {
                if f1_select.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_in_or_builder(fltrs));
                };
            },

            FilterS::NotIn(ref fltrs) => {
                if f1_select.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_not_in_or_builder(fltrs));
                };
            },

            FilterS::Eq(ref fltrs) => {
                if f1_select.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_eq_or_builder(fltrs));
                };
            } ,

            FilterS::Neq(ref fltrs) => {
                if f1_select.contains(&&*fltrs[0].0) {
                    f1 = f1.filter(fltr_neq_or_builder(fltrs));
                };
            },
        }
    }

    // Step 2.1 GROUPBY and 2.2 Calculate Measures
    // Need measure -> file mapping:
    // can be derived based on columns for basic measures
    // for bespoke need assumptions (eg "Delta")

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

    // Build Filter here to filter out lines where all measures are exactly 0
    
    //Build GROUPBY
    let groups: Vec<Expr> = req._groupby()
        .iter()
        .map(|x| { col(x) })
        .collect();
    
    // GROUPBY and AGGREGATE
    f1 = f1.groupby(groups)
        .agg(aggregateions);

    // POSTPROCESSING

    // Remove zeros, optional
    if req.optiona_params().as_ref()
        .and_then(|x|Some(x.hide_zeros))
        .unwrap_or_default() {
            let mut it = newnames.iter();
            let c = it.next().unwrap(); // Request should contain at least one measure
            // Filter where col is Not Eq 0 AND Not Eq Null
            let mut predicate = col(c).neq(lit::<f64>(0.)) .and(col(c).neq(NULL.lit()));
            for c in  it {
                predicate = predicate.or(col(c).neq(lit::<f64>(0.)). and(col(c).neq(NULL.lit())))
            }
            f1 = f1.filter(predicate);
        };
    
    f1.collect()
 }



/// This fn to be called per DataFrame
/// Builds aggregation expressions
fn agg_builder(measures_requested: &[(String, String)], 
    all_measures_map: Arc<MM>,
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


lazy_static!(
    pub static ref BASE_CALCS: HashMap<&'static str, fn(Expr, &str) -> (Expr, String)> = HashMap::from(
        [
        ("sum", sum as fn(Expr, &str) -> (Expr, String)),
        ("min", min),
        ("max", max),
        ("mean", mean),
        ("var", var),
        ("quantile95low", quantile_95_lower),
        ("first", first),
        ("list", list)
        ]
    );
);

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
fn first(c: Expr, newname: &str) -> (Expr, String) {
    // Not including "_first" alias to avoid confusion 
    // First is usually used by measures such as Capital or RiskWeight
    // Which are calculated at a level of a certain column such as RiskFactor 
    ( c.first().alias(newname), newname.to_string() )
}
fn list(c: Expr, newname: &str) -> (Expr, String) {
    let alias = format!("{newname}_list");
    ( c.list().alias(alias.as_ref()), alias)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}