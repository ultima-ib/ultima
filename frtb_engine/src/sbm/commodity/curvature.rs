use base_engine::prelude::OCP;
use crate::{prelude::*, sbm::equity::curvature::eq_curvature_charge};
use polars::prelude::*;

pub fn com_curv_delta (_: &OCP) -> Expr {
    curv_delta_total("Commodity")
}
/// Helper functions
pub fn com_curv_delta_weighted(op: &OCP) -> Expr {
    com_curv_delta(op)*col("CurvatureRiskWeight")
}
pub fn com_cvr_down(_: &OCP) -> Expr {
    rc_cvr("Commodity", CVR::Down)
}
pub fn com_cvr_up(_: &OCP) -> Expr {
    rc_cvr("Commodity", CVR::Up)
}
pub fn com_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Commodity", col("PnL_Up"))
}
pub fn com_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Commodity", col("PnL_Down"))    
}

pub(crate) fn com_curvature_kb_plus_low(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn com_curvature_kb_minus_low(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn com_curvature_kb_low(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn com_curvature_sb_low(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn com_curvature_charge_low(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn com_curvature_kb_plus_medium(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn com_curvature_kb_minus_medium(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn com_curvature_kb_medium(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn com_curvature_sb_medium(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn com_curvature_charge_medium(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

pub(crate) fn com_curvature_kb_plus_high(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn com_curvature_kb_minus_high(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn com_curvature_kb_high(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn com_curvature_sb_high(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn com_curvature_charge_high(op: &OCP) -> Expr {
    com_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn com_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();

    let com_curv_gamma = get_optional_parameter_array(op, format!("commodity_curv_gamma{_suffix}").as_str(), &scenario.com_gamma_curv);
    let com_curv_rho = get_optional_parameter(op, format!("commodity_curv_rho{_suffix}").as_str(), &scenario.com_curv_rho_cty);

    // Same methodology as EQ Curvature
    eq_curvature_charge(com_curv_rho.to_vec(), com_curv_gamma,  rtrn, "Commodity", None)
}