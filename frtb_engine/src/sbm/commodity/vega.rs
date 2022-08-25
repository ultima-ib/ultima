use base_engine::prelude::*;
use crate::{prelude::*, sbm::equity::vega::equity_vega_charge};

use polars::prelude::*;

pub fn total_com_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "Commodity", total_vega_curv_sens())
}

pub fn total_com_vega_sens_weighted (op: &OCP) -> Expr {
    total_com_vega_sens(op)*col("SensWeights").arr().get(0)
}
///Interm Result
pub(crate) fn com_vega_sb(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn com_vega_kb_low(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_low(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn com_vega_kb_medium(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_medium(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn com_vega_kb_high(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_high(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// TODO test
fn com_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    
    let com_gamma = get_optional_parameter_array(op, format!("commodity_delta_gamma{_suffix}").as_str(), &scenario.com_gamma);
    let com_rho_bucket = get_optional_parameter(op, format!("commodity_base_delta_rho_bucket{_suffix}").as_str(), &scenario.base_com_delta_rho_cty);
    let com_vega_rho = get_optional_parameter_array(op, format!("commodity_base_opt_mat_vega_rho{_suffix}").as_str(), &scenario.base_vega_rho);

    // The approach is identical to Equity
    equity_vega_charge(com_vega_rho, com_gamma, 
        com_rho_bucket.to_vec(), scenario.scenario_fn, rtrn, None, "Commodity")
}
