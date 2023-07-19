use crate::{
    datasource::DataSource, new::NewSourcedDataSet, read_toml2, DataSourceConfig, MeasuresMap,
};

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
/// TODO add reports
#[allow(clippy::uninlined_format_args)]
pub fn config_build_validate_prepare<DS: NewSourcedDataSet>(
    config_path: &str,
    bespoke_measures: MeasuresMap,
) -> DS {
    // Read Config
    let conf = read_toml2::<DataSourceConfig>(config_path)
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    let (source, measure_vec, config) = conf.build();

    let prepare = matches!(source, DataSource::InMemory(_));

    let mut mm = MeasuresMap::from_iter(measure_vec);
    mm.extend(bespoke_measures);

    let mut data = DS::new(source, mm, Default::default(), config);

    // Build DataSet

    // TODO
    // data.validate().expect();

    // Pre build some columns, which you wish to store in memory alongside the original data
    // Note if streaming then .prepare() should happen post filtering
    if prepare {
        data.prepare().expect("Failed to Prepare Frame");
        data.collect().expect("Failed to Prepare Frame");
    }

    data
}
