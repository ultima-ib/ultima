use base_engine::{DataSourceConfig, read_toml2, DataSetBase, DataSet};

#[test]
fn toml2config() {
    let conf_path = r"./tests/data/titanic_config.toml";
    assert!(read_toml2::<DataSourceConfig>(conf_path).is_ok())
}


#[test]
#[should_panic(expected = "Error reading file")]
fn config_build() {
    let conf_path = r"./tests/data/bad_config.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path)
        .expect("Can not proceed without valid Data Set Up");
    let mut data: DataSetBase = DataSet::build(conf);
    data.prepare();
}