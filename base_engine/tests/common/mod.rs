use std::env;

use once_cell::sync::Lazy;
use polars::prelude::*;

use base_engine::prelude::{read_toml2, DataSet, DataSetBase, DataSourceConfig};

pub static TEST_DASET: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let path = String::from(env!("CARGO_MANIFEST_DIR"));

    let conf_path = {
        let mut path = path.clone();
        path.push_str("/tests/data/test_config.toml");
        path
    };
    let conf = read_toml2::<DataSourceConfig>(conf_path.as_str())
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    let mut data: DataSetBase = DataSet::from_config_for_tests(conf, &path);
    data = data.prepare().unwrap();
    Arc::new(data)
});
