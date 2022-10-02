use crate::{prelude::*, sbm::csr_nonsec::vega::csr_nonsec_vega_charge};
use base_engine::prelude::*;

use polars::prelude::*;

pub fn total_csrsecctp_vega_sens(_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "CSR_Sec_CTP", total_vega_curv_sens())
}

pub fn total_csrsecctp_vega_sens_weighted(op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);

    match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => total_csrsecctp_vega_sens(op) * col("SensWeightsCRR2").arr().get(0),
        Jurisdiction::BCBS => total_csrsecctp_vega_sens(op) * col("SensWeights").arr().get(0),
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
fn csrsecctp_vega_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    let (weight, bucket_col, name_rho_vec, rho_opt, gamma, special_bucket) = match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (
            col("SensWeightsCRR2").arr().get(0),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_ctp_rho_name_crr2),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_gamma_crr2,
            None,
        ),

        Jurisdiction::BCBS => (
            col("SensWeights").arr().get(0),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_ctp_rho_name_bcbs),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_gamma,
            None,
        ),
    };

    let csr_gamma = get_optional_parameter_array(
        op,
        format!("csr_sec_ctp_vega_gamma{_suffix}").as_str(),
        gamma,
    );
    let base_csr_rho_bucket = get_optional_parameter_vec(
        op,
        format!("csr_sec_ctp_rho_diff_name_bucket{_suffix}").as_str(),
        &name_rho_vec,
    );
    let csr_vega_rho = get_optional_parameter_array(
        op,
        format!("csr_sec_ctp_opt_mat_vega_rho{_suffix}").as_str(),
        rho_opt,
    );

    csr_nonsec_vega_charge(
        weight,
        bucket_col,
        &scenario.scenario_fn,
        csr_vega_rho,
        base_csr_rho_bucket,
        csr_gamma,
        special_bucket,
        "CSR_Sec_CTP",
        "Vega",
        rtrn,
    )
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecctp_vega_max(op: &OCP) -> Expr {
    max_exprs(&[
        csrsecctp_vega_charge_low(op),
        csrsecctp_vega_charge_medium(op),
        csrsecctp_vega_charge_high(op),
    ])
}

/// Exporting Measures
pub(crate) fn csrsecctp_vega_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "CSR_secCTP_VegaSens".to_string(),
            calculator: Box::new(total_csrsecctp_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaSens_Weighted".to_string(),
            calculator: Box::new(total_csrsecctp_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaSb".to_string(),
            calculator: Box::new(csrsecctp_vega_sb),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaCharge_Low".to_string(),
            calculator: Box::new(csrsecctp_vega_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaKb_Low".to_string(),
            calculator: Box::new(csrsecctp_vega_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaCharge_Medium".to_string(),
            calculator: Box::new(csrsecctp_vega_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaKb_Medium".to_string(),
            calculator: Box::new(csrsecctp_vega_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaCharge_High".to_string(),
            calculator: Box::new(csrsecctp_vega_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaKb_High".to_string(),
            calculator: Box::new(csrsecctp_vega_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
        Measure {
            name: "CSR_secCTP_VegaCharge_MAX".to_string(),
            calculator: Box::new(csrsecctp_vega_max),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
        },
    ]
}
