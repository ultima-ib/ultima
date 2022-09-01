use crate::{prelude::*, sbm::equity::vega::equity_vega_charge};
use base_engine::prelude::*;

use polars::prelude::*;

pub fn total_csr_sec_nonctp_vega_sens(_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "CSR_Sec_nonCTP", total_vega_curv_sens())
}

pub fn total_csr_sec_nonctp_vega_sens_weighted(op: &OCP) -> Expr {
    total_csr_sec_nonctp_vega_sens(op) * col("SensWeights").arr().get(0)
}
///Interm Result
pub(crate) fn csr_sec_nonctp_vega_sb(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csr_sec_nonctp_vega_kb_low(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_low(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_sec_nonctp_vega_kb_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_sec_nonctp_vega_kb_high(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_high(op: &OCP) -> Expr {
    csr_sec_nonctp_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csr_sec_nonctp_vega_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();
    //TODO check
    let csr_sec_nonctp_gamma = get_optional_parameter_array(
        op,
        format!("csr_sec_nonctp_vega_gamma{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_gamma,
    );
    let csr_sec_nonctp_rho_bucket = get_optional_parameter(
        op,
        "base_csr_sec_nonctp_rho_diff_name_bucket",
        &scenario.base_csr_sec_nonctp_rho_diff_name,
    );
    let csr_sec_nonctp_vega_rho = get_optional_parameter_array(
        op,
        "base_csr_sec_nonctp_opt_mat_vega_rho",
        &scenario.base_vega_rho,
    );

    equity_vega_charge(
        csr_sec_nonctp_vega_rho,
        csr_sec_nonctp_gamma,
        csr_sec_nonctp_rho_bucket.to_vec(),
        scenario.scenario_fn,
        rtrn,
        Some("25"),
        "CSR_Sec_nonCTP",
    )
}

/// Exporting Measures
pub(crate) fn csrsecnonctp_vega_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "CSR_Sec_nonCTP_VegaSens".to_string(),
            calculator: Box::new(total_csr_sec_nonctp_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaSens_Weighted".to_string(),
            calculator: Box::new(total_csr_sec_nonctp_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaSb".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_sb),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaCharge_Low".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaKb_Low".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaCharge_Medium".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaKb_Medium".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaCharge_High".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_VegaKb_High".to_string(),
            calculator: Box::new(csr_sec_nonctp_vega_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
    ]
}