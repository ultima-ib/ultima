use std::{env, path::PathBuf};

use once_cell::sync::Lazy;
use polars::prelude::*;

use ultibi_core::{
    datasource::DataSource,
    new::NewSourcedDataSet,
    prelude::{read_toml2, DataSet, DataSetBase, DataSourceConfig},
    DependantMeasure, Measure, CPM,
};

#[allow(dead_code)] // Not dead code actually, but clippy complains
pub static TEST_DASET: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "data",
        "test_config.toml",
    ]
    .iter()
    .collect();
    let conf = read_toml2::<DataSourceConfig>(path.to_str().unwrap())
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    let mut data: DataSetBase = DataSetBase::from_config(conf);

    data.prepare().unwrap();
    Arc::new(data)
});

#[allow(dead_code)] // Not dead code actually, but clippy complains
pub static TEST_DASET_WITH_DEPENDANTS: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "testset.csv"]
        .iter()
        .collect();

    let df = LazyCsvReader::new(path).finish().unwrap();

    let measures = vec![
        Measure::Dependant(DependantMeasure {
            name: "DivAge".to_string(),
            calculator: Arc::new(|op: &CPM| {
                let n = op.get("count").unwrap().parse::<f64>().unwrap();
                Ok(col("Age_sum") / n.into())
            }),
            depends_upon: vec![("Age".to_string(), "sum".to_string())],
            calc_params: vec![],
        }),
        DependantMeasure {
            name: "NoSuchMeasureTest".to_string(),
            calculator: Arc::new(|op: &CPM| {
                let n = op.get("count").unwrap().parse::<f64>().unwrap();
                Ok(col("NoSuchMeasure_sum") / n.into())
            }),
            depends_upon: vec![("NoSuchMeasure".to_string(), "sum".to_string())],
            calc_params: vec![],
        }
        .into(),
    ];

    let data: DataSetBase = DataSetBase::from_vec(
        DataSource::Scan(df),
        measures,
        true,
        vec![],
        Default::default(),
    );

    // Not preparing here, since scan
    Arc::new(data)
});
