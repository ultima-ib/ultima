use std::time::Instant;

use crate::{read_toml2, DataSet, DataSourceConfig, MeasuresMap};

/// Reads initial DataSet from Source
///
/// TODO: calls .validate()
///
/// Then .prepare()
///
/// If streaming is False - also collects
///
/// *`collect` - indicates if DF should be collected
/// *`prepare` - indicates if DF should be prepared
/// *`bespoke_measures` - bespoke measures

#[allow(clippy::uninlined_format_args)]
pub fn config_build_validate_prepare<DS: DataSet>(
    config_path: &str,
    collect: bool,
    prepare: bool,
    bespoke_measures: MeasuresMap,
) -> impl DataSet {
    // Read Config
    let conf = read_toml2::<DataSourceConfig>(config_path)
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    let (lf, measure_vec, build_params) = conf.build();

    let mut mm = MeasuresMap::from_iter(measure_vec);
    mm.extend(bespoke_measures);

    let mut data = DS::new(lf, mm, build_params);

    // If cfg is streaming then we can't collect, otherwise collect to check errors
    if collect {
        let now = Instant::now();
        data.collect().expect("Failed to read frame");
        let elapsed = now.elapsed();
        println!("Time to Read/Aggregate DF: {:.6?}", elapsed);
    }

    // Build DataSet

    // TODO
    // data.validate().expect();

    // Pre build some columns, which you wish to store in memory alongside the original data
    // Note if streaming then .prepare() should happen post filtering
    if prepare {
        data.prepare().expect("Failed to Prepare Frame");
    }

    if collect {
        let now = Instant::now();
        data.collect().expect("Failed to Prepare Frame");
        let elapsed = now.elapsed();
        println!("Time to Prepare DF: {:.6?}", elapsed);
    }

    data
}
