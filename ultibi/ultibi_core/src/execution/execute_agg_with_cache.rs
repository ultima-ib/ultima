use polars::prelude::{col, DataFrame, Expr, IntoLazy, JoinType};

use crate::cache::CacheableDataSet;
use crate::errors::UltiResult;
use crate::{
    AggregationRequest, CacheableAggregationRequest, CacheableComputeRequest, ProcessedBaseMeasure,
};

/// TODO work in progress
/// Looks up from Cache
/// Whatever is not found is sent to [_exec_agg]
/// Whatever was sent to [_exec_agg] is then saved to Cache
/// Then results are joined
pub(crate) fn _exec_agg_with_cache<DS: CacheableDataSet + ?Sized>(
    data: &DS,
    req: AggregationRequest,
    processed_base_measures: Vec<(&String, &String, ProcessedBaseMeasure)>,
    streaming: bool,
) -> UltiResult<DataFrame> {
    let requested_groupby = req.groupby().clone();
    let grp_by_expr = req.groupby().iter().map(|x| col(x)).collect::<Vec<Expr>>();

    // have already been calculated
    let mut cached_results = vec![];
    // Need to be calculated
    let mut yet_to_calculate = vec![];

    // for each measure in req check cache
    //for cacheable_request in cacheable_requests {
    for (measure_name, agg_name, pbm) in processed_base_measures {
        let cacheable_request = CacheableAggregationRequest {
            measure: (measure_name.to_string(), agg_name.to_string()),
            name: req.name.clone(),
            groupby: req.groupby.clone(),
            filters: req.filters.clone(),
            overrides: req.overrides.clone(),
            add_row: req.add_row.clone(),
            calc_params: req.calc_params.clone(),
            totals: req.totals,
        };

        let cacheable_compute_request = CacheableComputeRequest::Aggregation(cacheable_request);

        match data.get_cache().get(&cacheable_compute_request) {
            // If found - store result
            Some(rf) => {
                cached_results.push(rf.value().clone());
            }
            // if not push to those which will have to be calculated
            _ => yet_to_calculate.push((cacheable_compute_request, pbm)),
        }
    }

    // retrieve cached results, put into a single DB
    let _chached_df = if !cached_results.is_empty() {
        let mut it = cached_results.into_iter();
        let mut res = it.next().unwrap(); //cached_res is not empty
        for df in it {
            res = res
                .lazy()
                .join(
                    df.lazy(),
                    grp_by_expr.clone(),
                    grp_by_expr.clone(),
                    JoinType::Outer.into(),
                )
                .collect()?
        }
        Some(res)
    } else {
        None
    };

    // Compute
    let _new_res = if !yet_to_calculate.is_empty() {
        // First, breakdown yet_to_calculate into: a) (cacheable_request, column_name) and b) processed_base_measures
        // a) will be used to save to cahche and lookup from the result
        // b) will be sent to _exec_agg_base
        let (processed_base_measures, cacheable_request_column_name): (
            Vec<ProcessedBaseMeasure>,
            Vec<(CacheableComputeRequest, String)>,
        ) = yet_to_calculate
            .into_iter()
            .map(|(cacheable_request, processed_base_measure)| {
                let column_name = processed_base_measure.name.clone();
                (processed_base_measure, (cacheable_request, column_name))
            })
            .unzip();

        //EXECUTE
        let new_res = super::_exec_agg_base(
            data,
            req.filters(),
            &req.add_row,
            &req.overrides,
            &req.groupby,
            req.totals,
            processed_base_measures,
            streaming,
        )?;

        // Now save each of new measures to cache
        cacheable_request_column_name
            .into_iter()
            .for_each(|(cacheable_request, column_name)| {
                let mut select = requested_groupby.clone();
                select.push(column_name);
                let new_res_df = new_res.select(&select).expect("Failed to look up new res"); // we should never fail here
                data.get_cache().insert(cacheable_request, new_res_df);
            });

        Some(new_res)
    } else {
        None
    };

    // Finally join cached and new if needed and return
    match (_chached_df, _new_res) {
        (Some(cached), Some(new)) => Ok(cached
            .lazy()
            .join(
                new.lazy(),
                grp_by_expr.clone(),
                grp_by_expr,
                JoinType::Outer.into(),
            )
            .collect()?),
        (None, Some(df)) | (Some(df), None) => Ok(df),
        _ => unreachable!(),
    }
}
