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
    data = data.prepare().unwrap();
    Arc::new(data)
});
