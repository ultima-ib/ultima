//! Main logic of execution in aggregate context

use polars::prelude::PolarsError;
pub use polars::{
    functions::diag_concat_df,
    prelude::{col, lit, DataFrame, Expr, IntoLazy, Literal, PolarsResult, NULL},
};

use crate::{
    add_row::df_from_maps_and_schema, filters::fltr_chain, helpers::diag_concat_lf,
    measure::base_measure_lookup, AggregationRequest, DataSet, ValidateSet,
};

/// main function which returns a Result of the calculation
/// currently support only the first element of frames
pub fn execute_aggregation<DS: DataSet + ?Sized>(
    req: &AggregationRequest,
    data: &DS,
    streaming: bool,
) -> PolarsResult<DataFrame> {
    // Step 0.1
    let mut f1 = data.get_lazyframe().clone();

    // Step 1.0 Applying FILTERS:
    // TODO check if column is present in DF - ( is this "second line of defence" even needed?)
    if let Some(f) = fltr_chain(req.filters()) {
        f1 = f1.filter(f)
    }

    // Steps 2.1 OVERRIDE, 2.2 Add Row

    // Step 2.1 Build AGGREGATIONS/Measures
    // First, parse requested measures
    let m = req.measures();
    if m.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "Select measures. What do you want to aggregate?".into(),
        ));
    }
    let op = req.calc_params();

    let measure_map = data.get_measures();
    let prepared_measures = base_measure_lookup(m, measure_map, op)?;

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
                measure_filter_opt = measure_filter_opt.map(|fltr| fltr.or(f)); //= measure_filter_opt.or(f);
            }
            // If at least one of the measure filters is None, then everything is true
            // and break
            None => {
                measure_filter_opt = None;
                break;
            }
        }
    }

    // Step 2.3 Applying (Mesure)FILTER
    if let Some(fltr) = measure_filter_opt {
        f1 = f1.filter(fltr)
    }

    // If streaming then prepare (assign weights) NOW (ie post filtering)
    if streaming {
        f1 = data.prepare_frame(Some(f1))?
    }

    // Step 2.4 Applying Overwrites
    for ow in req.overrides() {
        f1 = ow.lf_with_overwrite(f1)?
    }

    // Step 2.5 Add Row
    if !req.add_row.rows.is_empty() {
        let current_schema = f1.schema()?;
        let mut extra_frame = df_from_maps_and_schema(&req.add_row.rows, current_schema)?.lazy();

        if req.add_row.prepare {
            // Validating only a subset required for prepare()
            data.validate_frame(Some(&extra_frame), ValidateSet::SUBSET1)?;
            extra_frame = data.prepare_frame(Some(extra_frame))?;
        }
        f1 = diag_concat_lf([f1, extra_frame], true, true)?;
    }

    //dbg!(f1.clone().select([col("TradeId"), col("RiskClass"), col("RiskFactor"),col("BucketBCBS"), col("SensWeightsCRR2"), col("SensWeights")]).collect());
    //dbg!(f1.clone().select([col("*")]).collect());

    // Step 3.1 Build GROUPBY
    let groups: Vec<Expr> = req.groupby.iter().map(|x| col(x)).collect();
    // fill nulls with a "null" - needed for better totals views
    let groups_fill_nulls: Vec<Expr> = groups
        .clone()
        .into_iter()
        .map(|e| e.fill_null(lit(" ")))
        .collect();

    // Step 3.2 Apply GroupBy and Agg
    // Note .limit doesn't work with standard groupby on large frames
    // hence use groupby_stable
    let mut aggregated_df = f1
        .clone()
        .with_streaming(streaming)
        .groupby_stable(&groups)
        .agg(&aggregateions)
        .limit(1_000)
        .with_columns(&groups_fill_nulls)
        .collect()?;

    if req.totals & (groups.len() > 1) {
        let ordered_cols = aggregated_df.get_column_names_owned();
        let mut total_frames = vec![];
        //let mut with_cols = vec![];

        for i in (1..groups.len()).rev() {
            // Columns which we are going to aggregate this time
            // (groups minus it's last element)
            let grp_by = &groups[0..i];
            let grp_by_fill_null = &groups_fill_nulls[0..i];
            // Not doing this, since we otherwise loose soring
            //let last_gr_col_name = &req._groupby()[i];
            //with_cols.push(lit("Total").alias(last_gr_col_name));

            let _df = f1
                .clone()
                .with_streaming(streaming)
                .groupby_stable(grp_by)
                .agg(&aggregateions)
                .limit(100)
                .with_columns(grp_by_fill_null)
                .collect()?;
            total_frames.push(_df)
        }

        total_frames.push(aggregated_df);
        aggregated_df = diag_concat_df(&total_frames)?;

        let groups_totals: Vec<Expr> = groups
            .clone()
            .into_iter()
            .map(|e| e.fill_null(lit("Total")))
            .collect();

        aggregated_df = aggregated_df
            .lazy()
            .sort_by_exprs(&groups, vec![false; groups.len()], false)
            .select(ordered_cols.iter().map(|c| col(c)).collect::<Vec<Expr>>())
            .with_columns(groups_totals)
            .collect()?;
    }

    let mut res = aggregated_df.lazy();

    // POSTPROCESSING
    // Remove zeros, optional
    // TODO Note: Comparing with 0. doesn't work with list columns
    // TODO Need to check column type and based on that compare against 0. or not
    if req.hide_zeros {
        let mut it = newnames.iter();
        if let Some(c) = it.next() {
            // Filter where col is Not Eq 0 AND Not Eq Null
            let mut predicate = col(c).neq(lit::<f64>(0.)).and(col(c).neq(NULL.lit()));
            for c in it {
                predicate = predicate.or(col(c).neq(lit::<f64>(0.)).and(col(c).neq(NULL.lit())))
            }
            res = res.filter(predicate);
        }
    };

    res.collect()
}
