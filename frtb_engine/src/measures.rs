
use crate::sbm::common::sens_weights;
use crate::sbm::csr_nonsec::delta::{csr_nonsec_delta_charge_low, csr_nonsec_delta_charge_medium, csr_nonsec_delta_charge_high, total_csr_nonsec_delta_sens, csr_nonsec_delta_sens_weighted};
use crate::sbm::csr_sec_ctp::delta::{total_csr_sec_ctp_delta_sens, csr_sec_ctp_delta_sens_weighted, csr_sec_ctp_delta_charge_low, csr_sec_ctp_delta_charge_medium, csr_sec_ctp_delta_charge_high};
use crate::sbm::fx::delta::{fx_delta_sens, fx_delta_sens_weighted, fx_delta_charge_low, fx_delta_charge_medium, fx_delta_charge_high};
use crate::sbm::girr::delta::{total_ir_delta_sens, girr_delta_sens_weighted,
    girr_delta_charge_low, girr_delta_charge_medium, girr_delta_charge_high};
use crate::sbm::commodity::delta::{total_commodity_delta_sens, commodity_delta_sens_weighted, 
    commodity_delta_charge_low, commodity_delta_charge_medium, commodity_delta_charge_high};
use crate::sbm::equity::delta::{equity_delta_sens, equity_delta_sens_weighted,
        equity_delta_charge_low, equity_delta_charge_medium, equity_delta_charge_high};

use base_engine::prelude::*;

use once_cell::sync::Lazy;

/// Export measures
pub static FRTB_MEASURE_VEC: Lazy<Vec<Measure>>  = Lazy::new(|| {

    let all_sens_cols = vec!["SensitivitySpot", "Sensitivity_025Y", "Sensitivity_05Y", "Sensitivity_1Y",
    "Sensitivity_2Y", "Sensitivity_3Y", "Sensitivity_5Y", "Sensitivity_10Y", 
    "Sensitivity_15Y", "Sensitivity_20Y", "Sensitivity_30Y"];

    vec![
        // GIRR
        Measure{
            name: "GIRR_DeltaSens",
            calculator: Box::new(total_ir_delta_sens),
            req_columns: [vec!["RiskClass"], all_sens_cols.clone()].concat(),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaSens_Weighted",
            calculator: Box::new(girr_delta_sens_weighted),
            req_columns: [vec!["RiskClass"], all_sens_cols.clone()].concat(),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaCharge_Low",
            calculator: Box::new(girr_delta_charge_low),
            req_columns: [vec!["RiskClass", "RiskFactor", "RiskFactorType", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_Medium",
            calculator: Box::new(girr_delta_charge_medium),
            req_columns: [vec!["RiskClass", "RiskFactor", "RiskFactorType", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_High",
            calculator: Box::new(girr_delta_charge_high),
            req_columns: [vec!["RiskClass", "RiskFactor", "RiskFactorType", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        //FX
        Measure{
            name: "FX_DeltaSens",
            calculator: Box::new(fx_delta_sens),
            req_columns: vec!["RiskClass", "SensitivitySpot"],
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaSens_Weighted",
            calculator: Box::new(fx_delta_sens_weighted),
            req_columns: vec!["RiskClass", "SensitivitySpot", "SensWeights"],
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaCharge_Low",
            calculator: Box::new(fx_delta_charge_low),
            req_columns: vec!["RiskClass", "SensitivitySpot", "SensWeights", "BucketBCBS"],
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_Medium",
            calculator: Box::new(fx_delta_charge_medium),
            req_columns: vec!["RiskClass", "SensitivitySpot", "SensWeights", "BucketBCBS"],
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_High",
            calculator: Box::new(fx_delta_charge_high),
            req_columns: vec!["RiskClass", "SensitivitySpot", "SensWeights", "BucketBCBS"],
            aggregation: Some("first"),
        },

        //Commodity
        Measure {
            name: "Commodity_DeltaSens",
            calculator: Box::new(total_commodity_delta_sens),
            req_columns: [vec!["RiskClass"], all_sens_cols.clone()].concat(),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaSens_Weighted",
            calculator: Box::new(commodity_delta_sens_weighted),
            req_columns: [vec!["RiskClass"], all_sens_cols.clone()].concat(),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaCharge_Low",
            calculator: Box::new(commodity_delta_charge_low),
            req_columns: [vec!["RiskClass", "RiskFactor","SensWeights", "CommodityLocation", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_Medium",
            calculator: Box::new(commodity_delta_charge_medium),
            req_columns: [vec!["RiskClass", "RiskFactor","SensWeights", "CommodityLocation", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_High",
            calculator: Box::new(commodity_delta_charge_high),
            req_columns: [vec!["RiskClass", "RiskFactor","SensWeights", "CommodityLocation", "BucketBCBS"], all_sens_cols.clone()].concat(),
            aggregation: Some("first"),
        },

        // Equity
        Measure {
            name: "Equity_DeltaSens",
            calculator: Box::new(equity_delta_sens),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor","SensitivitySpot"],
            aggregation: None
        },

        Measure {
            name: "Equity_DeltaSens_Weighted",
            calculator: Box::new(equity_delta_sens_weighted),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor", "SensWeights", "SensitivitySpot"],
            aggregation: None,
        },

        Measure {
            name: "Equity_DeltaCharge_Low",
            calculator: Box::new(equity_delta_charge_low),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_Medium",
            calculator: Box::new(equity_delta_charge_medium),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_High",
            calculator: Box::new(equity_delta_charge_high),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        //CSR non-Sec

        Measure {
            name: "CSR_nonSec_DeltaSens",
            calculator: Box::new(total_csr_nonsec_delta_sens),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor","SensitivitySpot"],
            aggregation: None
        },

        Measure {
            name: "CSR_nonSec_DeltaSens_Weighted",
            calculator: Box::new(csr_nonsec_delta_sens_weighted),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor", "SensWeights", "SensitivitySpot"],
            aggregation: None,
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Low",
            calculator: Box::new(csr_nonsec_delta_charge_low),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Medium",
            calculator: Box::new(csr_nonsec_delta_charge_medium),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_High",
            calculator: Box::new(csr_nonsec_delta_charge_high),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        //CSR sec-CTP
        Measure {
            name: "CSR_secCTP_DeltaSens",
            calculator: Box::new(total_csr_sec_ctp_delta_sens),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor","SensitivitySpot"],
            aggregation: None
        },

        Measure {
            name: "CSR_secCTP_DeltaSens_Weighted",
            calculator: Box::new(csr_sec_ctp_delta_sens_weighted),
            req_columns: vec!["RiskCategory","RiskClass", "RiskFactor", "SensWeights", "SensitivitySpot"],
            aggregation: None,
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Low",
            calculator: Box::new(csr_sec_ctp_delta_charge_low),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Medium",
            calculator: Box::new(csr_sec_ctp_delta_charge_medium),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_High",
            calculator: Box::new(csr_sec_ctp_delta_charge_high),
            req_columns: vec!["RiskClass", "RiskFactor","SensWeights", "RiskFactorType", "BucketBCBS", "SensitivitySpot"],
            aggregation: Some("first"),
        },



        //Helpers

        //DeltaWeight view only makes sence at Bucket level
        //With exception of CSR non Sec where it is bucket and potentially CoveredBondReducedWeight
        Measure{
            name: "RiskWeights",
            calculator: Box::new(sens_weights),
            req_columns: vec!["SensWeights"],
            aggregation: Some("first"),
        },
    ]
});