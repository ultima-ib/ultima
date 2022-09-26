use std::time::Instant;

use base_engine::{DataSet, read_toml2, DataSourceConfig};
use log::info;

/// Reads initial DataSet from Source
/// 
/// Then calls .validate()
/// 
/// Then .prepare()
pub fn data <DS: DataSet>(config_path: &str) -> impl DataSet {
    // Read Config
    let conf =
        read_toml2::<DataSourceConfig>(config_path).expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    info!("Data SetUp: {:?}", conf);

    // Build data
    let mut data = DS::build(conf);
    // TODO
    // data.validate().expect();
    // Pre build some columns, which you wish to store in memory alongside the original data
    let now = Instant::now();
    data.prepare();
    let elapsed = now.elapsed();
    println!("Time to Prepare DF: {:.6?}", elapsed);
    data
}