use std::path::PathBuf;

use ultibi_core::{new::NewSourcedDataSet, read_toml2, DataSetBase, DataSourceConfig};

#[test]
fn toml2config() {
    let path = String::from(env!("CARGO_MANIFEST_DIR"));
    let conf_path = path + "/tests/data/bad_config.toml";
    read_toml2::<DataSourceConfig>(conf_path.as_str()).unwrap();
}

#[test]
#[should_panic(expected = "Error reading file")]
fn config_build() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend(["tests", "data", "bad_config.toml"]);

    let conf = read_toml2::<DataSourceConfig>(path.to_str().unwrap())
        .expect("Can not proceed without valid Data Set Up");
    let _data: DataSetBase = DataSetBase::from_config(conf);
}

/// In this config, files_join_attributes was provided but no such column is present
#[test]
#[should_panic(expected = "Check numeric columns in the config")]
fn config_build2() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend(["tests", "data", "bad_config2.toml"]);

    // TODO: test_config2.toml didn't exist
    let conf = read_toml2::<DataSourceConfig>(path.to_str().unwrap())
        .expect("Can not proceed without valid Data Set Up");
    let (_, _, _) = conf.build();
    //lf.collect().expect("Couldn't build");
}
