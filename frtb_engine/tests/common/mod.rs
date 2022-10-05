use once_cell::sync::Lazy;
use polars::prelude::*;

use base_engine::prelude::*;
use frtb_engine::prelude::*;

pub static LAZY_DASET: Lazy<Arc<FRTBDataSet>> = Lazy::new(|| {
    let conf_path = r"./tests/data/datasource_config.toml";
    let conf = read_toml2::<DataSourceConfig>(conf_path)
        .expect("Can not proceed without valid Data Set Up"); //Unrecovarable error
    let mut data: FRTBDataSet = DataSet::build(conf);
    data.prepare();
    Arc::new(data)
});

pub fn assert_results(req: &str, expected_sum: f64, epsilon: Option<f64>) {
    let ep = if let Some(e) = epsilon { e } else { 1e-5 };
    let data_req =
        serde_json::from_str::<AggregationRequest>(req).expect("Could not parse request");
    let excl = data_req._groupby().clone();
    let a = &*LAZY_DASET;
    let res =
        execute_aggregation(data_req, Arc::clone(a)).expect("Error while calculating results");
    let res_numeric = res
        .lazy()
        .select([col("*").exclude(excl)])
        .collect()
        .expect("Could not remove column");
    let res_arr = res_numeric
        .to_ndarray::<Float64Type>()
        .expect("Could not convert result to nd_array");
    // Slightly naive, but we assume if the sum is equivallent then the result is accurate
    dbg!(res_arr.sum());
    dbg!(expected_sum);
    assert!((res_arr.sum() - expected_sum).abs() < ep);
}