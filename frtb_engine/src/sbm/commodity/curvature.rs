use crate::{prelude::*, sbm::equity::curvature::eq_curvature_charge};
use base_engine::prelude::OCP;
use polars::prelude::*;

pub fn com_curv_delta(_: &OCP) -> Expr {
    curv_delta_total("Commodity")
}
/// Helper functions
pub fn com_curv_delta_weighted(op: &OCP) -> Expr {
    com_curv_delta(op) * col("CurvatureRiskWeight")
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

fn com_curvature_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();

    let com_curv_gamma = get_optional_parameter_array(
        op,
        format!("commodity_curv_gamma{_suffix}").as_str(),
        &scenario.com_gamma_curv,
    );
    let com_curv_rho = get_optional_parameter(
        op,
        format!("commodity_curv_rho{_suffix}").as_str(),
        &scenario.com_curv_rho_cty,
    );

    // Same methodology as EQ Curvature
    eq_curvature_charge(
        com_curv_rho.to_vec(),
        com_curv_gamma,
        rtrn,
        "Commodity",
        None,
    )
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn com_curv_max(op: &OCP) -> Expr {
    max_exprs(&[
        com_curvature_charge_low(op),
        com_curvature_charge_medium(op),
        com_curvature_charge_high(op),
    ])
}

/// Exporting Measures
pub(crate) fn com_curv_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "Commodity_CurvatureDelta".to_string(),
            calculator: Box::new(com_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CurvatureDelta_Weighted".to_string(),
            calculator: Box::new(com_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_PnLup".to_string(),
            calculator: Box::new(com_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_PnLdown".to_string(),
            calculator: Box::new(com_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CVRup".to_string(),
            calculator: Box::new(com_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CVRdown".to_string(),
            calculator: Box::new(com_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbPlus_Medium".to_string(),
            calculator: Box::new(com_curvature_kb_plus_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbMinus_Medium".to_string(),
            calculator: Box::new(com_curvature_kb_minus_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Kb_Medium".to_string(),
            calculator: Box::new(com_curvature_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Sb_Medium".to_string(),
            calculator: Box::new(com_curvature_sb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CurvatureCharge_Medium".to_string(),
            calculator: Box::new(com_curvature_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbPlus_Low".to_string(),
            calculator: Box::new(com_curvature_kb_plus_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbMinus_Low".to_string(),
            calculator: Box::new(com_curvature_kb_minus_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Kb_Low".to_string(),
            calculator: Box::new(com_curvature_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Sb_Low".to_string(),
            calculator: Box::new(com_curvature_sb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CurvatureCharge_Low".to_string(),
            calculator: Box::new(com_curvature_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbPlus_High".to_string(),
            calculator: Box::new(com_curvature_kb_plus_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_KbMinus_High".to_string(),
            calculator: Box::new(com_curvature_kb_minus_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Kb_High".to_string(),
            calculator: Box::new(com_curvature_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_Curvature_Sb_High".to_string(),
            calculator: Box::new(com_curvature_sb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CurvatureCharge_High".to_string(),
            calculator: Box::new(com_curvature_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_CurvatureCharge_MAX".to_string(),
            calculator: Box::new(com_curv_max),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
    ]
}
