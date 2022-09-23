//! Main logic of execution in aggregate context

use std::sync::Arc;

use polars::prelude::{PolarsResult, DataFrame, IntoLazy, Expr, lit, col, NULL, Literal};

use crate::{AggregationRequest, DataSet, measure_builder};


/// main function which returns a Result of the calculation
/// currently support only the first element of frames
pub fn execute_aggregation(req: AggregationRequest, data: Arc<impl DataSet + ?Sized>) -> PolarsResult<DataFrame>
{
    // Assuming Front End knows which columns can be in groupby, agg etc

    // Step 0.1
    let f1 = data.frame();
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