use base_engine::prelude::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;

pub fn total_csrnonsec_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "Equity", total_vega_curv_sens())
}

pub fn total_csrnonsec_vega_sens_weighted (op: &OCP) -> Expr {
    total_csrnonsec_vega_sens(op)*col("SensWeights").arr().get(0)
}


///calculate CSR Non Sec Interm Result
pub(crate) fn csr_nonsec_vega_sb(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_low(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_low(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_medium(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_medium(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_high(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_high(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csr_nonsec_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    //TODO check
    let csr_gamma = get_optional_parameter_array(op, format!("csr_vega_gamma{_suffix}").as_str(), &scenario.csr_nonsec_gamma);
    let base_csr_rho_bucket = get_optional_parameter(op, format!("csr_rho_diff_name_bucket{_suffix}").as_str(), &scenario.base_delta_eq_rho_bucket);
    let csr_vega_rho = get_optional_parameter_array(op, format!("csr_opt_mat_vega_rho{_suffix}").as_str(), &scenario.base_vega_rho);

    unimplemented!()
    //csr_nonsec_vega_charge(eq_vega_rho, eq_gamma, base_eq_rho_bucket, scenario.scenario_fn, rtrn)
}

