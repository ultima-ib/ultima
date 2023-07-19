//! Main logic of execution in aggregate context

use std::collections::{HashMap, HashSet};

use polars::prelude::{diag_concat_lf, PolarsError};
pub use polars::{
    functions::diag_concat_df,
    prelude::{col, lit, DataFrame, Expr, IntoLazy, Literal, PolarsResult, NULL},
};

use crate::{
    add_row::{df_from_maps_and_schema, AdditionalRows},
    agg_measure_lookup, agg_measure_to_expr,
    aggregations::{Aggregation, AggregationName, BASE_CALCS},
    errors::UltiResult,
    execute_agg_with_cache::_exec_agg_with_cache,
    filters::AndOrFltrChain,
    lookup_dependants_with_depth,
    overrides::Override,
    AggregationRequest, DataSet, Measure, MeasureName, ProcessedBaseMeasure, ProcessedMeasure,
};

/// Looks up measures and calls calculator on those returning an Expr
/// Breaks down requested measures into Basic and Dependents
/// Sends Basics to [ultibi::_exec_agg_with_cache] or [ultibi::_exec_agg_base]
/// Executes Dependents in .with_columns() context
pub fn exec_agg<DS: DataSet + ?Sized>(
    data: &DS,
    req: AggregationRequest,
    prepare: bool,
) -> UltiResult<DataFrame> {
    // Step 0: Lookup and return Expr
    let all_requested_measures = req.measures();
    if all_requested_measures.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "Select measures. What do you want to aggregate?".into(),
        )
        .into());
    }

    let op = req.calc_params(); // Optional params of the request

    let dataset_measure_map = data.get_measures(); // all availiable measures

    // Step 1.0 Lookup requested measures in the DataSet
    let looked_up_measures = agg_measure_lookup(all_requested_measures, dataset_measure_map)?;

    // Step 1.1 For dependants we need to keep track of their "depth"
    let dependants_with_depth =
        lookup_dependants_with_depth(all_requested_measures, dataset_measure_map);

    // Step 1.2 Express dependants now
    let mut processed_dependants = Vec::with_capacity(dependants_with_depth.len());
    let mut storage = HashSet::new();

    for i in dependants_with_depth {
        let mut inner = vec![];
        for (dm, agg) in i {
            // Check that dependant hasn't previously been invoked
            if storage.insert(&dm.name) {
                let calculator: Expr = agg.aggregate((dm.calculator)(op)?, &dm.name);
                inner.push(calculator)
            }
        }
        processed_dependants.push(inner);
    }

    // Remove duplicates
    let looked_up_measures_unique: HashMap<
        (&MeasureName, &AggregationName),
        (&Measure, &Aggregation),
    > = HashMap::from_iter(looked_up_measures);

    let expressed_measures = looked_up_measures_unique
        .into_iter()
        .map(
            |((measure_name, aggregation_name), (measure, aggregation))| {
                let expressed_measure = agg_measure_to_expr(measure, aggregation, op);
                match expressed_measure {
                    Ok(pm) => Ok((measure_name, aggregation_name, pm)),
                    Err(err) => Err(err),
                }
            },
        )
        .collect::<PolarsResult<Vec<(&MeasureName, &AggregationName, ProcessedMeasure)>>>()?;

    // Keep all REQUESTED Column Names for later use:
    let mut all_requested_columns_names = req.groupby().clone();
    all_requested_columns_names.extend(all_requested_measures.iter().map(|(measure_name, agg)| {
        let agg = BASE_CALCS.get(agg as &str).expect("Failed to look up agg"); //we have checked in agg_measure_lookup
        agg.new_name(measure_name as &str)
    }));
    // Keep cosmetic arguments for later use:
    let hide_zeros = req.hide_zeros;

    //  break down measures into dependant and basic
    let mut base_measures = Vec::with_capacity(expressed_measures.len());

    for m in expressed_measures {
        if let (measure_name, aggregation_name, ProcessedMeasure::Base(pbm)) = m {
            base_measures.push((measure_name, aggregation_name, pbm))
        }
    }

    // Step 2 Compute basics
    let mut res = match data.as_cacheable() {
        Some(cacheable) => _exec_agg_with_cache(cacheable, req.clone(), base_measures, prepare),
        _ => _exec_agg_base(
            data,
            req.filters(),
            &req.add_row,
            &req.overrides,
            &req.groupby,
            req.totals,
            base_measures.into_iter().map(|(_, _, pbm)| pbm).collect(),
            prepare,
        ),
    }?;

    // Step 3 compute dependants
    for i in processed_dependants.into_iter() {
        res = res.lazy().with_columns(i).collect()?
    }
    res = res
        .lazy()
        .select(
            all_requested_columns_names
                .iter()
                .map(|m| col(m))
                .collect::<Vec<Expr>>(),
        )
        .collect()?;

    // TODO Step 4 - cosmetics
    // Hide Zeros
    if hide_zeros {
        let is_numerc_col = res
            .columns(&all_requested_columns_names)?
            .into_iter()
            .map(|c| c._dtype().is_numeric())
            .collect::<Vec<bool>>();

        let mut it = all_requested_columns_names
            .into_iter()
            .zip(is_numerc_col.into_iter())
            .filter(|(_, y)| *y);

        if let Some((c, _)) = it.next() {
            // Filter where col is Not Eq 0 AND Not Eq Null
            let mut predicate = col(&c).neq(lit::<f64>(0.)).and(col(&c).neq(NULL.lit()));
            for (c, _) in it {
                predicate = predicate.or(col(&c).neq(lit::<f64>(0.)).and(col(&c).neq(NULL.lit())))
            }
            res = res.lazy().filter(predicate).collect()?;
        }
        //let all_numerics = Expr::DtypeColumn(vec![DataType::Float64]);
        //res = res.lazy().filter(all_numerics.clone().neq(lit::<f64>(0.)).and(all_numerics.neq(NULL.lit()))).collect()?;
    };

    Ok(res)
}

/// main function which returns a Result of the calculation
/// Executes base measures on your DataSet
pub(crate) fn _exec_agg_base<DS: DataSet + ?Sized>(
    data: &DS,
    filters: &AndOrFltrChain,
    add_rows: &AdditionalRows,
    overrides: &[Override],
    groupby: &[String],
    totals: bool,
    processed_base_measures: Vec<ProcessedBaseMeasure>,
    prepare: bool,
) -> UltiResult<DataFrame> {
    // Step 1.0 and 1.1 - get existing Filtered frame - first building block
    let mut f1 = data.get_lazyframe(filters);

    // Step 2.1
    // Unpack - (New Column Name, AggExpr, MeasureSpecificFilter)
    let (_, (aggregateions, fltrs)): (Vec<String>, (Vec<Expr>, Vec<Option<Expr>>)) =
        processed_base_measures
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
    if prepare {
        f1 = data.prepare_frame(f1)?
    }

    // Step 2.4 Applying Overwrites
    for ow in overrides {
        f1 = ow.lf_with_overwrite(f1)?
    }

    // Step 2.5 Add Row
    if !add_rows.rows.is_empty() {
        let current_schema = f1.schema()?;
        let mut extra_frame = df_from_maps_and_schema(&add_rows.rows, current_schema)?.lazy();

        if add_rows.prepare {
            // Validating only a subset required for prepare()
            data.validate_frame(Some(&extra_frame), 1)?;
            extra_frame = data.prepare_frame(extra_frame)?;
            // Collect now to reduce pressure on the logical plan
            // Totally Fine since extra_frame is always relatively small
            extra_frame = extra_frame.collect()?.lazy();
        }
        f1 = diag_concat_lf([f1, extra_frame], true, true)?;
    }

    //dbg!(f1.clone().select([col("TradeId"), col("Desk"), col("RiskFactor"),col("BucketBCBS"), col("SensWeightsCRR2"), col("SensWeights")]).collect());
    //dbg!(f1.clone().select([col("*")]).collect());
    //dbg!(&groupby);

    // Step 3.1 Build GROUPBY
    let groups: Vec<Expr> = groupby.iter().map(|x| col(x)).collect();
    // fill nulls with a "null" - needed for better totals views
    let groups_fill_nulls: Vec<Expr> = groups
        .clone()
        .into_iter()
        .map(|e| e.fill_null(lit(" ")))
        .collect();

    //let f1 = f1.collect()?.lazy();

    // Step 3.2 Apply GroupBy and Agg
    // Note .limit doesn't work with standard groupby on large frames
    // hence use groupby_stable
    let mut aggregated_df = f1
        .clone()
        .with_streaming(true) // Set streaming to True anyway - no performance penalty
        .groupby_stable(&groups)
        .agg(&aggregateions)
        .limit(1_000)
        .with_columns(&groups_fill_nulls)
        .collect()?;

    if totals & (groups.len() > 1) {
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
                .with_streaming(true)
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
            .sort_by_exprs(&groups, vec![false; groups.len()], false, true)
            .select(ordered_cols.iter().map(|c| col(c)).collect::<Vec<Expr>>())
            .with_columns(groups_totals)
            .collect()?;
    }

    Ok(aggregated_df)
}
