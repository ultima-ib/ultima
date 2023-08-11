//! Example of a driver with SQL DataSource
//! 
use ultibi::datasource::DataSource;
use ultibi::new::NewSourcedDataSet;
use ultibi::polars::prelude::{Schema, Field, DataType};

use std::sync::{Arc, RwLock};

use ultibi::{DataSet, VisualDataSet, AggregationRequest};
use frtb_engine::FRTBDataSet;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;
#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;
#[global_allocator]
#[cfg(not(target_os = "linux"))]
static ALLOC: MiMalloc = MiMalloc;

#[allow(clippy::uninlined_format_args)]
fn main() -> anyhow::Result<()> {
    // Read .env
    dotenv::dotenv().ok();

    // Assume non streaming mode
    // For more information see documentation

    //let data = config_build_validate_prepare::<DataSetType>(setup_path.as_str(), default);
    let uri=format!("mysql://{0}:{1}@{2}:{3}/{4}?cxprotocol=binary",
        "dummyUser" , "password", "localhost", 3306, "ultima");

    let request: AggregationRequest = serde_json::from_str(request()).expect("Bad requests");

    let datasource  = DataSource::Db(ultibi::datasource::DbInfo { table: "frtb".to_string(), 
        db_type: "MySQL".to_string(),
         conn_uri: uri,
         schema: Some(hardcoded_schema().into())}) ;

    let dataset = FRTBDataSet::from_vec(datasource, vec![],
    true, vec![],
    Default::default());

    let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(dataset));

    //ds.ui();

    //dbg!(
        dbg!(ds.read().unwrap().compute(request.into()).expect("COMPUTE FAILED"))
    //)
    ;

    Ok(())
}

/// Hardcoded schema of the table
fn hardcoded_schema() -> Schema {

    let fields = vec![
                Field::new("COB", DataType::Utf8),
        Field::new("TradeId", DataType::Utf8),
        Field::new("RiskCategory", DataType::Utf8),
        Field::new("RiskClass", DataType::Utf8),
        Field::new("RiskFactor", DataType::Utf8),
        Field::new("RiskFactorType", DataType::Utf8),
        Field::new("CreditQuality", DataType::Utf8),
        Field::new("MaturityDate", DataType::Utf8),
        Field::new("Tranche", DataType::Utf8),
        Field::new("CommodityLocation", DataType::Utf8),
        Field::new("GirrVegaUnderlyingMaturity", DataType::Utf8),
        Field::new("BucketBCBS", DataType::Utf8),
        Field::new("BucketCRR2", DataType::Utf8),
        Field::new("GrossJTD", DataType::Float64),
        Field::new("PnL_Up", DataType::Float64),
        Field::new("PnL_Down", DataType::Float64),
        Field::new("SensitivitySpot", DataType::Float64),
        Field::new("Sensitivity_025Y", DataType::Float64),
        Field::new("Sensitivity_05Y", DataType::Float64),
        Field::new("Sensitivity_1Y", DataType::Float64),
        Field::new("Sensitivity_2Y", DataType::Float64),
        Field::new("Sensitivity_3Y", DataType::Float64),
        Field::new("Sensitivity_5Y", DataType::Float64),
        Field::new("Sensitivity_10Y", DataType::Float64),
        Field::new("Sensitivity_15Y", DataType::Float64),
        Field::new("Sensitivity_20Y", DataType::Float64),
        Field::new("Sensitivity_30Y", DataType::Float64),
        Field::new("SensitivityCcy", DataType::Utf8),
        Field::new("CoveredBondReducedWeight", DataType::Utf8),
        Field::new("Sector", DataType::Utf8),
        Field::new("FxCurvDivEligibility", DataType::Boolean),
        Field::new("BookId", DataType::Utf8),
        Field::new("Product", DataType::Utf8),
        Field::new("EXOTIC_RRAO", DataType::Boolean),
        Field::new("OTHER_RRAO", DataType::Boolean),
        Field::new("Notional", DataType::Float64),
        Field::new("Desk", DataType::Utf8),
        Field::new("Country", DataType::Utf8),
        Field::new("LegalEntity", DataType::Utf8),
        Field::new("Group", DataType::Utf8),
    ];

    Schema::from_iter(fields)

}

// dbg
fn request() -> &'static str {
    r#"{
        "filters": [],
        "groupby": [
          "COB"
        ],
        "measures": [
          [
            "SA Charge",
            "scalar"
          ]
        ],
        "overrides": [],
        "hide_zeros": false,
        "totals": false,
        "calc_params": {},
        "additionalRows": {
          "prepare": false,
          "rows": []
        }
      }"#
}