use std::{env, path::PathBuf};

use once_cell::sync::Lazy;
use polars::prelude::*;

use base_engine::prelude::{read_toml2, DataSet, DataSetBase, DataSourceConfig};

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

    let mut data: DataSetBase = DataSet::from_config_for_tests(conf, path.to_str().unwrap());
    data.prepare().unwrap();
    Arc::new(data)
});

pub static TEST_DASET_WITH_DEPENDANTS: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "data",
        "testset.csv",
    ]
    .iter()
    .collect();

    let conf = read_toml2::<DataSourceConfig>(path.to_str().unwrap())
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    let df = LazyCsvReader::new(path).finish().unwrap();
    // TODO build pattern
    // TODO build_params should be passed to .prepare() as an arg
    
    //let mut data: DataSetBase = DataSet::new();

    let mut data: DataSetBase = DataSet::from_config_for_tests(conf, path.to_str().unwrap());
    data.prepare().unwrap();
    Arc::new(data)
});
