use base_engine::prelude::OCP;
use crate::{prelude::*, sbm::csr_nonsec::curvature::csrnonsec_curvature_charge};
use polars::prelude::*;

pub fn csr_sec_nonctp_curv_delta (_: &OCP) -> Expr {
    curv_delta_5("CSR_Sec_nonCTP")
}
/// Helper functions
pub fn csr_sec_nonctp_curv_delta_weighted(op: &OCP) -> Expr {
    csr_sec_nonctp_curv_delta(op)*col("CurvatureRiskWeight")
}
pub fn csr_sec_nonctp_cvr_down(_: &OCP) -> Expr {
    rc_cvr_5("CSR_Sec_nonCTP", CVR::Down)
}
pub fn csr_sec_nonctp_cvr_up(_: &OCP) -> Expr {
    rc_cvr_5("CSR_Sec_nonCTP", CVR::Up)
}
pub fn csr_sec_nonctp_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_Sec_nonCTP", col("PnL_Up"))
}
pub fn csr_sec_nonctp_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_Sec_nonCTP", col("PnL_Down"))    
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_low(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_low(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_low(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csr_sec_nonctp_curvature_sb_low(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csr_sec_nonctp_curvature_charge_low(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csr_sec_nonctp_curvature_sb_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csr_sec_nonctp_curvature_charge_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_high(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_high(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csr_sec_nonctp_curvature_kb_high(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csr_sec_nonctp_curvature_sb_high(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csr_sec_nonctp_curvature_charge_high(op: &OCP) -> Expr {
    csr_sec_nonctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn csr_sec_nonctp_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();

    let curv_gamma = get_optional_parameter_array(op, format!("csr_sec_nonctp_curv_gamma{_suffix}").as_str(), &scenario.csr_sec_nonctp_gamma_curv);
    let curv_rho = get_optional_parameter(op, format!("csr_sec_nonctp_curv_rho{_suffix}").as_str(), &scenario.csr_sec_nonctp_rho_diff_name_curv);

    csrnonsec_curvature_charge(curv_rho.to_vec(), 
    curv_gamma, 
    rtrn,
    Some(25),
    col("CurvatureRiskWeight"),
    col("BucketBCBS"),
    "CSR_Sec_nonCTP",)
}
