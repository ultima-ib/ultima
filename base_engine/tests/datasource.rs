mod datasource_tests {
    use base_engine::{read_toml2, DataSet, DataSetBase, DataSourceConfig};

    #[test]
    fn toml2config() {
        let path = String::from(env!("CARGO_MANIFEST_DIR"));
        let conf_path = path + "/tests/data/bad_config.toml";
        read_toml2::<DataSourceConfig>(conf_path.as_str()).unwrap();
    }

    #[test]
    #[should_panic(expected = "Error reading file")]
    fn config_build() {
        let path = String::from(env!("CARGO_MANIFEST_DIR"));
        let conf_path = path + "/tests/data/bad_config.toml";

        let conf = read_toml2::<DataSourceConfig>(conf_path.as_str())
            .expect("Can not proceed without valid Data Set Up");
        let mut _data: DataSetBase = DataSet::from_config(conf);
    }

    /// In this config, files_join_attributes was provided but no such column is present
    #[test]
    #[should_panic(expected = "Couldn't build")]
    fn config_build2() {
        let path = String::from(env!("CARGO_MANIFEST_DIR"));
        let conf_path = path.clone() + "/tests/data/bad_config2.toml";

        let mut conf = read_toml2::<DataSourceConfig>(conf_path.as_str())
            .expect("Can not proceed without valid Data Set Up");

        conf.change_path_on_abs_if_not_exist(&path);

        let (lf, _, _) = conf.build();
        lf.collect().expect("Couldn't build");
    }
}
