use std::{env, path::PathBuf};

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

pub static TEST_DASET1: Lazy<Arc<DataSetBase>> = Lazy::new(|| {
    let path = env::current_dir().unwrap().to_str().unwrap().to_string();

    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("resources/test");
    println!("{}", d.display());

    dbg!(&path);

    let conf_path = path + r"/base_engine/tests/data/test_config_dbg.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path.as_str())
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error

    // d.push("resources/test");
    let mut data: DataSetBase = DataSet::from_config(conf);
    data = data.prepare().unwrap();
    Arc::new(data)
});
