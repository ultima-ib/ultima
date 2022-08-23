use base_engine::prelude::*;
use crate::{prelude::*, sbm::csr_nonsec::vega::csr_nonsec_vega_charge};

use polars::prelude::*;

pub fn total_csrsecctp_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "CSR_Sec_CTP", total_vega_curv_sens())
}

pub fn total_csrsecctp_vega_sens_weighted (op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 =>total_csrsecctp_vega_sens(op)*col("SensWeightsCRR2").arr().get(0),
        Jurisdiction::BCBS =>total_csrsecctp_vega_sens(op)*col("SensWeights").arr().get(0)
    }
}

///calculate CSR Sec CTP Interm Result
pub(crate) fn csrsecctp_vega_sb(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_low(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_low(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_medium(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_medium(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_high(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_high(op: &OCP) -> Expr {
    csrsecctp_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csrsecctp_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    let (weight, bucket_col, name_rho_vec,
        rho_opt, 
        gamma,
        n_buckets, special_bucket) =
        match juri{
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (
            col("SensWeightsCRR2").arr().get(0),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_ctp_rho_name_crr2),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_gamma_crr2,
            18usize, 
            None
            ),

            Jurisdiction::BCBS=>
            (
            col("SensWeights").arr().get(0),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_ctp_rho_name_bcbs),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_gamma,
            16,
            None
            )
        };

    let csr_gamma = get_optional_parameter_array(op, format!("csr_sec_ctp_vega_gamma{_suffix}").as_str(), gamma);
    let base_csr_rho_bucket = get_optional_parameter_vec(op, format!("csr_sec_ctp_rho_diff_name_bucket{_suffix}").as_str(), &name_rho_vec);
    let csr_vega_rho = get_optional_parameter_array(op, format!("csr_sec_ctp_opt_mat_vega_rho{_suffix}").as_str(), rho_opt);

    csr_nonsec_vega_charge(weight, bucket_col, &scenario.scenario_fn, 
        csr_vega_rho, base_csr_rho_bucket, 
        csr_gamma, n_buckets, special_bucket, "CSR_Sec_CTP", "Vega", rtrn)
}
