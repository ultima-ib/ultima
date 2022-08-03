
use crate::sbm::common::sens_weights;
use crate::sbm::csr_nonsec::delta::{csr_nonsec_delta_charge_low, csr_nonsec_delta_charge_medium, csr_nonsec_delta_charge_high, total_csr_nonsec_delta_sens, csr_nonsec_delta_sens_weighted};
use crate::sbm::csr_sec_ctp::delta::{total_csr_sec_ctp_delta_sens, csr_sec_ctp_delta_sens_weighted, csr_sec_ctp_delta_charge_low, csr_sec_ctp_delta_charge_medium, csr_sec_ctp_delta_charge_high};
use crate::sbm::csr_sec_nonctp::delta::{total_csr_sec_nonctp_delta_sens, csr_sec_nonctp_delta_sens_weighted, csr_sec_nonctp_delta_charge_low, csr_sec_nonctp_delta_charge_medium, csr_sec_nonctp_delta_charge_high};
use crate::sbm::fx::delta::{fx_delta_sens, fx_delta_sens_weighted, fx_delta_charge_low, fx_delta_charge_medium, fx_delta_charge_high};
use crate::sbm::girr::curvature::{ir_curv_delta, girr_curv_delta_weighted, girr_cvr_up, girr_cvr_down, girr_pnl_up, girr_pnl_down, girr_curvature_charge_medium, girr_curvature_kb_plus, girr_curvature_kb_minus, girr_curvature_kb, girr_curvature_sb, girr_curvature_charge_low, girr_curvature_charge_high};
use crate::sbm::girr::delta::{total_ir_delta_sens, girr_delta_sens_weighted,
    girr_delta_charge_low, girr_delta_charge_medium, girr_delta_charge_high, girr_delta_sb, girr_delta_kb_low, girr_delta_kb_medium, girr_delta_kb_high};
use crate::sbm::commodity::delta::{total_commodity_delta_sens, commodity_delta_sens_weighted, 
    commodity_delta_charge_low, commodity_delta_charge_medium, commodity_delta_charge_high};
use crate::sbm::equity::delta::{equity_delta_sens, equity_delta_sens_weighted,
        equity_delta_charge_low, equity_delta_charge_medium, equity_delta_charge_high};
use crate::sbm::girr::vega::{total_ir_vega_sens, girr_vega_sens_weighted, girr_vega_charge_medium, girr_vega_kb_medium, girr_vega_kb_high, girr_vega_charge_high, girr_vega_charge_low, girr_vega_kb_low, girr_vega_sb};

use base_engine::prelude::*;

use once_cell::sync::Lazy;

/// Export measures
pub static FRTB_MEASURE_VEC: Lazy<Vec<Measure>>  = Lazy::new(|| {

    //let all_sens_cols = vec!["SensitivitySpot", "Sensitivity_025Y", "Sensitivity_05Y", "Sensitivity_1Y",
    //"Sensitivity_2Y", "Sensitivity_3Y", "Sensitivity_5Y", "Sensitivity_10Y", 
    //"Sensitivity_15Y", "Sensitivity_20Y", "Sensitivity_30Y"];

    vec![
        //                                                 ##### Delta #####
        // GIRR
        Measure{
            name: "GIRR_DeltaSens",
            calculator: Box::new(total_ir_delta_sens),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaSens_Weighted",
            calculator: Box::new(girr_delta_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaSb",
            calculator: Box::new(girr_delta_sb),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_Low",
            calculator: Box::new(girr_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_Low",
            calculator: Box::new(girr_delta_kb_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_Medium",
            calculator: Box::new(girr_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_Medium",
            calculator: Box::new(girr_delta_kb_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_High",
            calculator: Box::new(girr_delta_charge_high),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_High",
            calculator: Box::new(girr_delta_kb_high),
            aggregation: Some("first"),
        },

        //FX
        Measure{
            name: "FX_DeltaSens",
            calculator: Box::new(fx_delta_sens),
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaSens_Weighted",
            calculator: Box::new(fx_delta_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaCharge_Low",
            calculator: Box::new(fx_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_Medium",
            calculator: Box::new(fx_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_High",
            calculator: Box::new(fx_delta_charge_high),
            aggregation: Some("first"),
        },

        //Commodity
        Measure {
            name: "Commodity_DeltaSens",
            calculator: Box::new(total_commodity_delta_sens),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaSens_Weighted",
            calculator: Box::new(commodity_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaCharge_Low",
            calculator: Box::new(commodity_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_Medium",
            calculator: Box::new(commodity_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_High",
            calculator: Box::new(commodity_delta_charge_high),
            aggregation: Some("first"),
        },

        // Equity
        Measure {
            name: "Equity_DeltaSens",
            calculator: Box::new(equity_delta_sens),
            aggregation: None
        },

        Measure {
            name: "Equity_DeltaSens_Weighted",
            calculator: Box::new(equity_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "Equity_DeltaCharge_Low",
            calculator: Box::new(equity_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_Medium",
            calculator: Box::new(equity_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_High",
            calculator: Box::new(equity_delta_charge_high),
            aggregation: Some("first"),
        },

        //CSR non-Sec

        Measure {
            name: "CSR_nonSec_DeltaSens",
            calculator: Box::new(total_csr_nonsec_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_nonSec_DeltaSens_Weighted",
            calculator: Box::new(csr_nonsec_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Low",
            calculator: Box::new(csr_nonsec_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Medium",
            calculator: Box::new(csr_nonsec_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_High",
            calculator: Box::new(csr_nonsec_delta_charge_high),
            aggregation: Some("first"),
        },

        //CSR sec-CTP
        Measure {
            name: "CSR_secCTP_DeltaSens",
            calculator: Box::new(total_csr_sec_ctp_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_secCTP_DeltaSens_Weighted",
            calculator: Box::new(csr_sec_ctp_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Low",
            calculator: Box::new(csr_sec_ctp_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Medium",
            calculator: Box::new(csr_sec_ctp_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_High",
            calculator: Box::new(csr_sec_ctp_delta_charge_high),
            aggregation: Some("first"),
        },
        //CSR Sec non-CTP
        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens",
            calculator: Box::new(total_csr_sec_nonctp_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens_Weighted",
            calculator: Box::new(csr_sec_nonctp_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Low",
            calculator: Box::new(csr_sec_nonctp_delta_charge_low),
            aggregation: None
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Medium",
            calculator: Box::new(csr_sec_nonctp_delta_charge_medium),
            aggregation: None,
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_High",
            calculator: Box::new(csr_sec_nonctp_delta_charge_high),
            aggregation: None,
        },

        //                                                   ##### Vega #####
        // GIRR 
        Measure{
            name: "GIRR_VegaSens",
            calculator: Box::new(total_ir_vega_sens),
            aggregation: None,
        },

        Measure{
            name: "GIRR_VegaSens_Weighted",
            calculator: Box::new(girr_vega_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_VegaSb",
            calculator: Box::new(girr_vega_sb),
            aggregation: Some("first"),
        },
        
        Measure{
            name: "GIRR_VegaCharge_Low",
            calculator: Box::new(girr_vega_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_Low",
            calculator: Box::new(girr_vega_kb_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaCharge_Medium",
            calculator: Box::new(girr_vega_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_Medium",
            calculator: Box::new(girr_vega_kb_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaCharge_High",
            calculator: Box::new(girr_vega_charge_high),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_High",
            calculator: Box::new(girr_vega_kb_high),
            aggregation: Some("first"),
        },

        // ##############################Curvature##############################
        Measure{
            name: "GIRR_CurvatureDelta",
            calculator: Box::new(ir_curv_delta),
            aggregation: None,
        },

        Measure{
            name: "GIRR_PnLup",
            calculator: Box::new(girr_pnl_up),
            aggregation: None,
        },

        Measure{
            name: "GIRR_PnLdown",
            calculator: Box::new(girr_pnl_down),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CurvatureDelta_Weighted",
            calculator: Box::new(girr_curv_delta_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CVRup",
            calculator: Box::new(girr_cvr_up),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CVRdown",
            calculator: Box::new(girr_cvr_down),
            aggregation: None,
        },

        Measure{
            name: "GIRR_Curvature_KbPlus",
            calculator: Box::new(girr_curvature_kb_plus),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_Curvature_KbMinus",
            calculator: Box::new(girr_curvature_kb_minus),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_Curvature_Kb",
            calculator: Box::new(girr_curvature_kb),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_Curvature_Sb",
            calculator: Box::new(girr_curvature_sb),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_Low",
            calculator: Box::new(girr_curvature_charge_low),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_Medium",
            calculator: Box::new(girr_curvature_charge_medium),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_High",
            calculator: Box::new(girr_curvature_charge_high),
            aggregation: Some("first"),
        },

        //Helpers

        //Risk Weight view only makes sence at Bucket level
        //With exception of CSR non Sec where it is bucket and potentially CoveredBondReducedWeight
        Measure{
            name: "RiskWeights",
            calculator: Box::new(sens_weights),
            aggregation: Some("first"),
        },
    ]
});