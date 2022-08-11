
//! This file defines all the measures, associated with this library

use crate::sbm::common::sens_weights;
use crate::sbm::csr_nonsec::delta::{csr_nonsec_delta_charge_low, csr_nonsec_delta_charge_medium, csr_nonsec_delta_charge_high, total_csr_nonsec_delta_sens, csr_nonsec_delta_sens_weighted};
use crate::sbm::csr_sec_ctp::delta::{total_csr_sec_ctp_delta_sens, csr_sec_ctp_delta_sens_weighted, csr_sec_ctp_delta_charge_low, csr_sec_ctp_delta_charge_medium, csr_sec_ctp_delta_charge_high};
use crate::sbm::csr_sec_nonctp::delta::{total_csr_sec_nonctp_delta_sens, csr_sec_nonctp_delta_sens_weighted, csr_sec_nonctp_delta_charge_low, csr_sec_nonctp_delta_charge_medium, csr_sec_nonctp_delta_charge_high};
use crate::sbm::equity::vega::{total_eq_vega_sens, total_eq_vega_sens_weighted, equity_vega_charge_medium, equity_vega_sb, equity_vega_kb_medium, equity_vega_kb_low, equity_vega_charge_low, equity_vega_kb_high, equity_vega_charge_high};
use crate::sbm::fx::curvature::{fx_curv_delta, fx_pnl_up, fx_pnl_down, fx_curv_delta_weighted, fx_cvr_up, fx_cvr_down, fx_curvature_kb_plus, fx_curvature_kb_minus, fx_curvature_kb, fx_curvature_sb, fx_curvature_charge_low, fx_curvature_charge_medium, fx_curvature_charge_high};
use crate::sbm::fx::delta::{fx_delta_sens_repccy, fx_delta_sens_weighted, fx_delta_charge_low, fx_delta_charge_medium, fx_delta_charge_high, fx_delta_sb, fx_delta_kb};
use crate::sbm::fx::vega::{total_fx_vega_sens, total_fx_vega_sens_weighted, fx_vega_sb, fx_vega_kb_medium, fx_vega_kb_low, fx_vega_kb_high, fx_vega_charge_low, fx_vega_charge_medium, fx_vega_charge_high};
use crate::sbm::girr::curvature::{ir_curv_delta, girr_curv_delta_weighted, girr_cvr_up, girr_cvr_down, girr_pnl_up, girr_pnl_down, girr_curvature_charge_medium, girr_curvature_kb_plus, girr_curvature_kb_minus, girr_curvature_kb, girr_curvature_sb, girr_curvature_charge_low, girr_curvature_charge_high};
use crate::sbm::girr::delta::{total_ir_delta_sens, girr_delta_sens_weighted,
    girr_delta_charge_low, girr_delta_charge_medium, girr_delta_charge_high, girr_delta_sb, girr_delta_kb_low, girr_delta_kb_medium, girr_delta_kb_high};
use crate::sbm::commodity::delta::{total_commodity_delta_sens, commodity_delta_sens_weighted, 
    commodity_delta_charge_low, commodity_delta_charge_medium, commodity_delta_charge_high};
use crate::sbm::equity::delta::{equity_delta_sens, equity_delta_sens_weighted,
        equity_delta_charge_low, equity_delta_charge_medium, equity_delta_charge_high, eq_delta_kb_low, eq_delta_kb_medium, eq_delta_kb_high, eq_delta_sb};
use crate::sbm::girr::vega::{total_ir_vega_sens, girr_vega_sens_weighted, girr_vega_charge_medium, girr_vega_kb_medium, girr_vega_kb_high, girr_vega_charge_high, girr_vega_charge_low, girr_vega_kb_low, girr_vega_sb};

use base_engine::prelude::*;

/// Export measures
pub(crate)fn frtb_measure_vec() -> Vec<Measure<'static>> {

    vec![
        //                             ############################## FX Delta ##############################
        Measure{
            name: "FX_DeltaSens".to_string(),
            calculator: Box::new(fx_delta_sens_repccy),
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaSens_Weighted".to_string(),
            calculator: Box::new(fx_delta_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "FX_DeltaSb".to_string(),
            calculator: Box::new(fx_delta_sb),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaKb".to_string(),
            calculator: Box::new(fx_delta_kb),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_Low".to_string(),
            calculator: Box::new(fx_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_Medium".to_string(),
            calculator: Box::new(fx_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_DeltaCharge_High".to_string(),
            calculator: Box::new(fx_delta_charge_high),
            aggregation: Some("first"),
        },
        //        ################################# FX Vega ######################################
        Measure{
            name: "FX_VegaSens".to_string(),
            calculator: Box::new(total_fx_vega_sens),
            aggregation: None,
        },
        Measure{
            name: "FX_VegaSens_Weighted".to_string(),
            calculator: Box::new(total_fx_vega_sens_weighted),
            aggregation: None,
        },
        Measure{
            name: "FX_VegaSb".to_string(),
            calculator: Box::new(fx_vega_sb),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaKb_Low".to_string(),
            calculator: Box::new(fx_vega_kb_low),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaKb_Medium".to_string(),
            calculator: Box::new(fx_vega_kb_medium),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaKb_High".to_string(),
            calculator: Box::new(fx_vega_kb_high),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaCharge_Low".to_string(),
            calculator: Box::new(fx_vega_charge_low),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaCharge_Medium".to_string(),
            calculator: Box::new(fx_vega_charge_medium),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_VegaCharge_High".to_string(),
            calculator: Box::new(fx_vega_charge_high),
            aggregation: Some("first"),
        },
        //        ################################ FX Curvature ##############################
        Measure{
            name: "FX_CurvatureDelta".to_string(),
            calculator: Box::new(fx_curv_delta),
            aggregation: None,
        },
        Measure{
            name: "FX_CurvatureDelta_Weighted".to_string(),
            calculator: Box::new(fx_curv_delta_weighted),
            aggregation: None,
        },
        Measure{
            name: "FX_PnLup".to_string(),
            calculator: Box::new(fx_pnl_up),
            aggregation: None,
        },

        Measure{
            name: "FX_PnLdown".to_string(),
            calculator: Box::new(fx_pnl_down),
            aggregation: None,
        },
        Measure{
            name: "FX_CVRup".to_string(),
            calculator: Box::new(fx_cvr_up),
            aggregation: None,
        },

        Measure{
            name: "FX_CVRdown".to_string(),
            calculator: Box::new(fx_cvr_down),
            aggregation: None,
        },
        Measure{
            name: "FX_Curvature_KbPlus".to_string(),
            calculator: Box::new(fx_curvature_kb_plus),
            aggregation: Some("first"),
        },

        Measure{
            name: "FX_Curvature_KbMinus".to_string(),
            calculator: Box::new(fx_curvature_kb_minus),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_Curvature_Kb".to_string(),
            calculator: Box::new(fx_curvature_kb),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_Curvature_Sb".to_string(),
            calculator: Box::new(fx_curvature_sb),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_CurvatureCharge_Low".to_string(),
            calculator: Box::new(fx_curvature_charge_low),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_CurvatureCharge_Medium".to_string(),
            calculator: Box::new(fx_curvature_charge_medium),
            aggregation: Some("first"),
        },
        Measure{
            name: "FX_CurvatureCharge_High".to_string(),
            calculator: Box::new(fx_curvature_charge_high),
            aggregation: Some("first"),
        },

        //        ################################ Commodity Delta ##############################
        Measure {
            name: "Commodity_DeltaSens".to_string(),
            calculator: Box::new(total_commodity_delta_sens),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaSens_Weighted".to_string(),
            calculator: Box::new(commodity_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "Commodity_DeltaCharge_Low".to_string(),
            calculator: Box::new(commodity_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_Medium".to_string(),
            calculator: Box::new(commodity_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Commodity_DeltaCharge_High".to_string(),
            calculator: Box::new(commodity_delta_charge_high),
            aggregation: Some("first"),
        },

        // ######################### Equity Delta #######################################
        Measure {
            name: "Equity_DeltaSens".to_string(),
            calculator: Box::new(equity_delta_sens),
            aggregation: None
        },

        Measure {
            name: "Equity_DeltaSens_Weighted".to_string(),
            calculator: Box::new(equity_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "Equity_DeltaSb".to_string(),
            calculator: Box::new(eq_delta_sb),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaKb_Low".to_string(),
            calculator: Box::new(eq_delta_kb_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaKb_Medium".to_string(),
            calculator: Box::new(eq_delta_kb_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaKb_High".to_string(),
            calculator: Box::new(eq_delta_kb_high),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_Low".to_string(),
            calculator: Box::new(equity_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_Medium".to_string(),
            calculator: Box::new(equity_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_DeltaCharge_High".to_string(),
            calculator: Box::new(equity_delta_charge_high),
            aggregation: Some("first"),
        },

        // ######################### Equity Vega #######################################
        Measure {
            name: "Equity_VegaSens".to_string(),
            calculator: Box::new(total_eq_vega_sens),
            aggregation: None
        },

        Measure {
            name: "Equity_VegaSens_Weighted".to_string(),
            calculator: Box::new(total_eq_vega_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "Equity_VegaSb".to_string(),
            calculator: Box::new(equity_vega_sb),
            aggregation: Some("first"),
        },
        Measure {
            name: "Equity_VegaKb_Low".to_string(),
            calculator: Box::new(equity_vega_kb_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_VegaCharge_Low".to_string(),
            calculator: Box::new(equity_vega_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_VegaKb_Medium".to_string(),
            calculator: Box::new(equity_vega_kb_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_VegaCharge_Medium".to_string(),
            calculator: Box::new(equity_vega_charge_medium),
            aggregation: Some("first"),
        },
        Measure {
            name: "Equity_VegaKb_High".to_string(),
            calculator: Box::new(equity_vega_kb_high),
            aggregation: Some("first"),
        },

        Measure {
            name: "Equity_VegaCharge_High".to_string(),
            calculator: Box::new(equity_vega_charge_high),
            aggregation: Some("first"),
        },

        //CSR non-Sec
        Measure {
            name: "CSR_nonSec_DeltaSens".to_string(),
            calculator: Box::new(total_csr_nonsec_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_nonSec_DeltaSens_Weighted".to_string(),
            calculator: Box::new(csr_nonsec_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Low".to_string(),
            calculator: Box::new(csr_nonsec_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_Medium".to_string(),
            calculator: Box::new(csr_nonsec_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_nonSec_DeltaCharge_High".to_string(),
            calculator: Box::new(csr_nonsec_delta_charge_high),
            aggregation: Some("first"),
        },

        //CSR sec-CTP
        Measure {
            name: "CSR_secCTP_DeltaSens".to_string(),
            calculator: Box::new(total_csr_sec_ctp_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_secCTP_DeltaSens_Weighted".to_string(),
            calculator: Box::new(csr_sec_ctp_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Low".to_string(),
            calculator: Box::new(csr_sec_ctp_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_Medium".to_string(),
            calculator: Box::new(csr_sec_ctp_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure {
            name: "CSR_secCTP_DeltaCharge_High".to_string(),
            calculator: Box::new(csr_sec_ctp_delta_charge_high),
            aggregation: Some("first"),
        },
        //CSR Sec non-CTP
        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens".to_string(),
            calculator: Box::new(total_csr_sec_nonctp_delta_sens),
            aggregation: None
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens_Weighted".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_sens_weighted),
            aggregation: None,
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Low".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_low),
            aggregation: None
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Medium".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_medium),
            aggregation: None,
        },

        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_High".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_high),
            aggregation: None,
        },
        //                      ################################ GIRR Delta #######################################
        Measure{
            name: "GIRR_DeltaSens".to_string(),
            calculator: Box::new(total_ir_delta_sens),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaSens_Weighted".to_string(),
            calculator: Box::new(girr_delta_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_DeltaSb".to_string(),
            calculator: Box::new(girr_delta_sb),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_Low".to_string(),
            calculator: Box::new(girr_delta_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_Low".to_string(),
            calculator: Box::new(girr_delta_kb_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_Medium".to_string(),
            calculator: Box::new(girr_delta_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_Medium".to_string(),
            calculator: Box::new(girr_delta_kb_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaCharge_High".to_string(),
            calculator: Box::new(girr_delta_charge_high),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_DeltaKb_High".to_string(),
            calculator: Box::new(girr_delta_kb_high),
            aggregation: Some("first"),
        },

        //                      ################################ GIRR Vega ####################################### 
        Measure{
            name: "GIRR_VegaSens".to_string(),
            calculator: Box::new(total_ir_vega_sens),
            aggregation: None,
        },

        Measure{
            name: "GIRR_VegaSens_Weighted".to_string(),
            calculator: Box::new(girr_vega_sens_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_VegaSb".to_string(),
            calculator: Box::new(girr_vega_sb),
            aggregation: Some("first"),
        },
        
        Measure{
            name: "GIRR_VegaCharge_Low".to_string(),
            calculator: Box::new(girr_vega_charge_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_Low".to_string(),
            calculator: Box::new(girr_vega_kb_low),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaCharge_Medium".to_string(),
            calculator: Box::new(girr_vega_charge_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_Medium".to_string(),
            calculator: Box::new(girr_vega_kb_medium),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaCharge_High".to_string(),
            calculator: Box::new(girr_vega_charge_high),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_VegaKb_High".to_string(),
            calculator: Box::new(girr_vega_kb_high),
            aggregation: Some("first"),
        },

        //                      ################################ GIRR Curvature #######################################
        Measure{
            name: "GIRR_CurvatureDelta".to_string(),
            calculator: Box::new(ir_curv_delta),
            aggregation: None,
        },

        Measure{
            name: "GIRR_PnLup".to_string(),
            calculator: Box::new(girr_pnl_up),
            aggregation: None,
        },

        Measure{
            name: "GIRR_PnLdown".to_string(),
            calculator: Box::new(girr_pnl_down),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CurvatureDelta_Weighted".to_string(),
            calculator: Box::new(girr_curv_delta_weighted),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CVRup".to_string(),
            calculator: Box::new(girr_cvr_up),
            aggregation: None,
        },

        Measure{
            name: "GIRR_CVRdown".to_string(),
            calculator: Box::new(girr_cvr_down),
            aggregation: None,
        },

        Measure{
            name: "GIRR_Curvature_KbPlus".to_string(),
            calculator: Box::new(girr_curvature_kb_plus),
            aggregation: Some("first"),
        },

        Measure{
            name: "GIRR_Curvature_KbMinus".to_string(),
            calculator: Box::new(girr_curvature_kb_minus),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_Curvature_Kb".to_string(),
            calculator: Box::new(girr_curvature_kb),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_Curvature_Sb".to_string(),
            calculator: Box::new(girr_curvature_sb),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_Low".to_string(),
            calculator: Box::new(girr_curvature_charge_low),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_Medium".to_string(),
            calculator: Box::new(girr_curvature_charge_medium),
            aggregation: Some("first"),
        },
        Measure{
            name: "GIRR_CurvatureCharge_High".to_string(),
            calculator: Box::new(girr_curvature_charge_high),
            aggregation: Some("first"),
        },

        //Helpers

        //Risk Weight view only makes sence at Bucket level
        //With exception of CSR non Sec where it is bucket and potentially CoveredBondReducedWeight
        Measure{
            name: "RiskWeights".to_string(),
            calculator: Box::new(sens_weights),
            aggregation: Some("first"),
        },
    ]
}