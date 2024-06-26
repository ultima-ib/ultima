//! Example of a driver with SQL DataSource

use ultibi::polars::prelude::{DataType, Field, Schema};

use std::collections::BTreeMap;

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
    let _uri = format!(
        "mysql://{0}:{1}@{2}:{3}/{4}?cxprotocol=binary",
        "dummyUser", "password", "localhost", 3306, "ultima"
    );

    //let request: AggregationRequest = serde_json::from_str(request()).expect("Bad requests");

    #[cfg(feature = "db")]
    {
        use frtb_engine::FRTBDataSet;
        use std::sync::{Arc, RwLock};
        use ultibi::datasource::DataSource;
        use ultibi::new::NewSourcedDataSet;
        use ultibi::{DataSet, VisualDataSet};

        let datasource = DataSource::Db(ultibi::datasource::DbInfo {
            table: "frtb".to_string(),
            db_type: "MySQL".to_string(),
            conn_uri: _uri,
            schema: _hardcoded_schema().into(),
        });

        let dataset = FRTBDataSet::from_vec(datasource, vec![], true, vec![], _params());

        let ds: Arc<RwLock<dyn DataSet>> = Arc::new(RwLock::new(dataset));

        ds.ui();
    }
    //dbg!(
    // dbg!(ds.read().unwrap().compute(request.into()).expect("COMPUTE FAILED"))
    //);

    Ok(())
}

/// Hardcoded schema of the table
fn _hardcoded_schema() -> Schema {
    let fields = vec![
        Field::new("COB", DataType::String),
        Field::new("TradeId", DataType::String),
        Field::new("RiskCategory", DataType::String),
        Field::new("RiskClass", DataType::String),
        Field::new("RiskFactor", DataType::String),
        Field::new("RiskFactorType", DataType::String),
        Field::new("CreditQuality", DataType::String),
        Field::new("MaturityDate", DataType::String),
        Field::new("Tranche", DataType::String),
        Field::new("CommodityLocation", DataType::String),
        Field::new("GirrVegaUnderlyingMaturity", DataType::String),
        Field::new("BucketBCBS", DataType::String),
        Field::new("BucketCRR2", DataType::String),
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
        Field::new("SensitivityCcy", DataType::String),
        Field::new("CoveredBondReducedWeight", DataType::String),
        Field::new("Sector", DataType::String),
        Field::new("FxCurvDivEligibility", DataType::Boolean),
        Field::new("BookId", DataType::String),
        Field::new("Product", DataType::String),
        Field::new("EXOTIC_RRAO", DataType::Boolean),
        Field::new("OTHER_RRAO", DataType::Boolean),
        Field::new("Notional", DataType::Float64),
        Field::new("Desk", DataType::String),
        Field::new("Country", DataType::String),
        Field::new("LegalEntity", DataType::String),
        Field::new("Group", DataType::String),
    ];

    Schema::from_iter(fields)
}

// // dbg
// fn request() -> &'static str {
//     r#"{
//         "filters": [[{"op": "Eq", "field": "Desk", "value": "FXOptions"}]],
//         "groupby": [
//           "Desk"
//         ],
//         "measures": [
//             ["FX DeltaCharge Low", "scalar"],
//             ["FX DeltaCharge Medium", "scalar"],
//             ["FX DeltaCharge High", "scalar"]
//         ],
//         "overrides": [],
//         "hide_zeros": false,
//         "totals": false,
//         "calc_params": {
//             "apply_fx_curv_div": "true",
//             "reporting_ccy": "USD",
//             "drc_offset": "true",
//             "jurisdiction": "BCBS"
//         },
//         "additionalRows": {
//           "prepare": false,
//           "rows": []
//         }
//       }"#
// }

fn _params() -> BTreeMap<String, String> {
    BTreeMap::from_iter([
        ("fx_sqrt2_div".into(),  "true".into()),
        ("vega_risk_weights".into(), "./tests/data/vega_risk_weights.csv".into()),
        ("girr_delta_base_weights".into(),  "{\"columns\":[{\"name\":\"RiskClass\",\"datatype\":\"Utf8\",\"values\":[\"GIRR\",\"GIRR\",\"GIRR\"]},{\"name\":\"RiskCategory\",\"datatype\":\"Utf8\",\"values\":[\"Delta\",\"Delta\",\"Delta\"]},{\"name\":\"RiskFactorType\",\"datatype\":\"Utf8\",\"values\":[\"Yield\",\"Inflation\",\"XCCY\"]},{\"name\":\"Weights\",\"datatype\":\"Utf8\",\"values\":[\"0.0;0.017;0.017;0.016;0.013;0.012;0.011;0.011;0.011;0.011;0.011\",\"0.016\",\"0.016\"]}]}".into()),
        ("girr_sqrt2_div".into(), "true".into()),
        ("csrnonsec_covered_bond_15".into(), "true".into()),
        ("DayCountConvention".into(), "2".into()),
        ("DateFormat".into(), "%d/%m/%Y".into()),
    ])
}
