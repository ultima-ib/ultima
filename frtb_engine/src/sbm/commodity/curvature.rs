use crate::{
    prelude::{
        get_optional_parameter, get_optional_parameter_array, ReturnMetric, ScenarioConfig,
        HIGH_CORR_SCENARIO, LOW_CORR_SCENARIO, MEDIUM_CORR_SCENARIO,
    },
    sbm::{
        common::rc_rcat_sens,
        common_curv::{curv_delta_total, rc_cvr, Cvr},
        equity::curvature::eq_curvature_charge,
    },
};
use ultibi::lit;
use ultibi::polars::lazy::dsl::{col, max_horizontal, Expr};
use ultibi::{prelude::CPM, BaseMeasure, Measure, PolarsResult};

pub fn com_curv_delta(_: &CPM) -> PolarsResult<Expr> {
    Ok(curv_delta_total("Commodity"))
}
/// Helper functions
pub fn com_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    com_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeight"))
}
pub fn com_cvr_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr("Commodity", Cvr::Down))
}
pub fn com_cvr_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr("Commodity", Cvr::Up))
}
pub fn com_pnl_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "Commodity", col("PnL_Up")))
}
pub fn com_pnl_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "Commodity", col("PnL_Down")))
}

pub(crate) fn com_curvature_kb_plus_low(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn com_curvature_kb_minus_low(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn com_curvature_kb_low(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn com_curvature_sb_low(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn com_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn com_curvature_kb_plus_medium(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn com_curvature_kb_minus_medium(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn com_curvature_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn com_curvature_sb_medium(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn com_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn com_curvature_kb_plus_high(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn com_curvature_kb_minus_high(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn com_curvature_kb_high(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn com_curvature_sb_high(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn com_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    com_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn com_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();

    let com_curv_gamma = get_optional_parameter_array(
        op,
        format!("commodity_curv_gamma{_suffix}").as_str(),
        &scenario.com_curv_gamma,
    )?;
    let com_curv_rho = get_optional_parameter(
        op,
        format!("com_curv_diff_name_rho_per_bucket{_suffix}").as_str(),
        &scenario.com_curv_diff_name_rho_per_bucket,
    )?;

    // Same methodology as EQ Curvature
    Ok(eq_curvature_charge(
        com_curv_rho.to_vec(),
        com_curv_gamma,
        rtrn,
        "Commodity",
        None,
    ))
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn com_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        com_curvature_charge_low(op)?,
        com_curvature_charge_medium(op)?,
        com_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn com_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(com_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureDelta Weighted".to_string(),
            calculator: std::sync::Arc::new(com_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity PnLup".to_string(),
            calculator: std::sync::Arc::new(com_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity PnLdown".to_string(),
            calculator: std::sync::Arc::new(com_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CVRup".to_string(),
            calculator: std::sync::Arc::new(com_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CVRdown".to_string(),
            calculator: std::sync::Arc::new(com_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbPlus Medium".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_plus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbMinus Medium".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_minus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Kb Medium".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Sb Medium".to_string(),
            calculator: std::sync::Arc::new(com_curvature_sb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(com_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbPlus Low".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_plus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbMinus Low".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_minus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Kb Low".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Sb Low".to_string(),
            calculator: std::sync::Arc::new(com_curvature_sb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(com_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbPlus High".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_plus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature KbMinus High".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_minus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Kb High".to_string(),
            calculator: std::sync::Arc::new(com_curvature_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity Curvature Sb High".to_string(),
            calculator: std::sync::Arc::new(com_curvature_sb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(com_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "Commodity CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(com_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
            calc_params: vec![],
        }),
    ]
}
