use base_engine::prelude::OCP;
use crate::{prelude::*, sbm::csr_nonsec::curvature::csrnonsec_curvature_charge};
use polars::prelude::*;

pub fn csrsecctp_curv_delta (_: &OCP) -> Expr {
    curv_delta_5("CSR_Sec_CTP")
}
/// Helper functions
pub fn csrsecctp_curv_delta_weighted(op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 =>csrsecctp_curv_delta(op)*col("CurvatureRiskWeightCRR2"),
        Jurisdiction::BCBS =>csrsecctp_curv_delta(op)*col("CurvatureRiskWeight"),
    }
}

pub fn csrsecctp_cvr_down(_: &OCP) -> Expr {
    rc_cvr_5("CSR_Sec_CTP", CVR::Down)
}
pub fn csrsecctp_cvr_up(_: &OCP) -> Expr {
    rc_cvr_5("CSR_Sec_CTP", CVR::Up)
}
pub fn csrsecctp_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_Sec_CTP", col("PnL_Up"))
}
pub fn csrsecctp_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_Sec_CTP", col("PnL_Down"))    
}

pub(crate) fn csrsecctp_curvature_kb_plus_low(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrsecctp_curvature_kb_minus_low(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrsecctp_curvature_kb_low(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrsecctp_curvature_sb_low(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrsecctp_curvature_charge_low(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csrsecctp_curvature_kb_plus_medium(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrsecctp_curvature_kb_minus_medium(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrsecctp_curvature_kb_medium(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrsecctp_curvature_sb_medium(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrsecctp_curvature_charge_medium(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

pub(crate) fn csrsecctp_curvature_kb_plus_high(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrsecctp_curvature_kb_minus_high(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrsecctp_curvature_kb_high(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrsecctp_curvature_sb_high(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrsecctp_curvature_charge_high(op: &OCP) -> Expr {
    csrsecctp_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn csrsecctp_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    let juri: Jurisdiction = get_jurisdiction(op);

    let (weight, bucket_col, name_rho_vec,
        gamma,
        special_bucket) =
        match juri{
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (
            col("CurvatureRiskWeightCRR2"),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_ctp_rho_name_crr2_curv),
            &scenario.csr_ctp_gamma_crr2_curv,
            None
            ),

            Jurisdiction::BCBS=>
            (
            col("CurvatureRiskWeight"),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_ctp_rho_name_bcbs_curv),
            &scenario.csr_ctp_gamma_curv,
            None
            )
        };

    let csr_secctp_curv_gamma = get_optional_parameter_array(op, format!("csr_secctp_curv_gamma{_suffix}").as_str(), &gamma);
    let csr_secctp_curv_rho = get_optional_parameter_vec(op, format!("csr_secctp_curv_rho{_suffix}").as_str(), &name_rho_vec);
    
    
    csrnonsec_curvature_charge(csr_secctp_curv_rho, csr_secctp_curv_gamma,
          rtrn, special_bucket, weight, bucket_col, "CSR_Sec_CTP")
}