use once_cell::sync::Lazy;
use polars::prelude::{DataFrame, PolarsError, PolarsResult};
use schnellru::{ByLength, LruMap};
use std::sync::RwLock;

use crate::{execute_aggregation, AggregationRequest, DataSet};

// //TODO: may be use enum instead of string?
#[derive(Debug, Clone, Hash, PartialEq)]
struct Measure(String, String);

// #[derive(Debug)]
// enum MeasureType {
//     Basic(Measure),    // (eg FX DeltaCharge Low)
//     Sum(Vec<Measure>), // (eg sbm_charge)
//     Max(Vec<Measure>), // (eg sbm_charge_high)
// }

// TODO: may be replace to ByMemoryUsage?
static GLOBAL_CACHE: Lazy<RwLock<LruMap<Measure, DataFrame>>> =
    Lazy::new(|| RwLock::new(LruMap::new(ByLength::new(100))));

// TODO: WIP!!!
pub fn execute_with_cache<DS: DataSet + ?Sized>(
    req: &AggregationRequest,
    data: &DS,
    streaming: bool,
) -> PolarsResult<DataFrame> {
    let measures: Vec<Measure> = req
        .measures
        .clone()
        .into_iter()
        .map(|s| Measure(s.0, s.1))
        .collect();

    let mut cached_results: Vec<(Measure, DataFrame)> = vec![];
    let mut non_cached_measures: Vec<Measure> = vec![];

    // Get Write lock on Read cause modify cashe range
    if let Ok(mut cache) = GLOBAL_CACHE.write() {
        for measure in measures {
            if let Some(cached) = cache.get(&measure) {
                cached_results.push((measure, cached.clone()));
            } else {
                non_cached_measures.push(measure);
            }
        }
    } else {
        return Err(PolarsError::NoData("Cache didnt exist.".into()));
    }

    // exec non-cachet measures
    let mut req = req.clone();
    let mut calculated_non_cached: Vec<(Measure, Result<_, _>)> = non_cached_measures
        .into_iter()
        .map(|measure| {
            // TODO: execution method for one measure needed
            req.measures = vec![(measure.0.clone(), measure.1.clone())];
            (measure, execute_aggregation(&req, data, streaming))
        })
        .collect();

    // return first exec error
    // TODO: find a more elegant solution than index after impl execute_one_measure
    let first_err_idx = calculated_non_cached
        .iter()
        .enumerate()
        .find(|(_, (_, res))| res.is_err())
        .map(|(i, _)| i);

    if let Some(i) = first_err_idx {
        let (_, res) = calculated_non_cached.remove(i);
        return res;
    }

    // insert to cache calculated df-s
    if let Ok(mut cache) = GLOBAL_CACHE.write() {
        for (measure, result) in calculated_non_cached {
            let df = result.unwrap();
            if cache.insert(measure.clone(), df.clone()) {
                cached_results.push((measure, df));
            }
        }
    }

    // TODO: join cached_results

    // unimplemented!()
    Err(PolarsError::NoData("WIP".into()))
}

pub fn get_cache_size() -> Option<usize> {
    if let Ok(cache) = GLOBAL_CACHE.read() {
        Some(cache.len())
    } else {
        None
    }
}
