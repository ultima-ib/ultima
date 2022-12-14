use once_cell::sync::Lazy;
use polars::prelude::*;

use base_engine::prelude::{read_toml2, DataSet, DataSetBase, DataSourceConfig};

pub static TEST_DASET: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let conf_path = r"./tests/data/test_config.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path)
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    let mut data: DataSetBase = DataSet::from_config(conf);
    data = data.prepare().unwrap();
    Arc::new(data)
});
