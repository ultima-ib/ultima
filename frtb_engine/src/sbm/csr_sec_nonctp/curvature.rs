use crate::{prelude::*, sbm::csr_nonsec::curvature::csrnonsec_curvature_charge};
use polars::prelude::*;
use ultibi::{prelude::CPM, BaseMeasure};

pub fn csr_sec_nonctp_curv_delta(_: &CPM) -> PolarsResult<Expr> {
    Ok(curv_delta_5("CSR_Sec_nonCTP"))
}
/// Helper functions
pub fn csr_sec_nonctp_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeight"))
}
pub fn csr_sec_nonctp_cvr_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_5("CSR_Sec_nonCTP", Cvr::Down))
}
pub fn csr_sec_nonctp_cvr_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_5("CSR_Sec_nonCTP", Cvr::Up))
}
pub fn csr_sec_nonctp_pnl_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_Sec_nonCTP", col("PnL_Up")))
}
pub fn csr_sec_nonctp_pnl_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_Sec_nonCTP", col("PnL_Down")))
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csr_sec_nonctp_curvature_sb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csr_sec_nonctp_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csr_sec_nonctp_curvature_sb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csr_sec_nonctp_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(
        op,
        &MEDIUM_CORR_SCENARIO,
        ReturnMetric::CapitalCharge,
    )
}

pub(crate) fn csr_sec_nonctp_curvature_kb_plus_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_minus_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csr_sec_nonctp_curvature_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csr_sec_nonctp_curvature_sb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csr_sec_nonctp_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_curvature_charge_distributor(
        op,
        &HIGH_CORR_SCENARIO,
        ReturnMetric::CapitalCharge,
    )
}

fn csr_sec_nonctp_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();

    let curv_gamma = get_optional_parameter_array(
        op,
        format!("csr_sec_nonctp_curv_gamma{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_curv_gamma,
    )?;
    let curv_rho = get_optional_parameter(
        op,
        format!("csr_sec_nonctp_curv_diff_name_rho_per_bucket{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_curv_diff_name_rho_per_bucket,
    )?;

    Ok(csrnonsec_curvature_charge(
        curv_rho.to_vec(),
        curv_gamma,
        rtrn,
        Some(25),
        col("CurvatureRiskWeight"),
        col("BucketBCBS"),
        "CSR_Sec_nonCTP",
    ))
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecnonctp_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csr_sec_nonctp_curvature_charge_low(op)?,
        csr_sec_nonctp_curvature_charge_medium(op)?,
        csr_sec_nonctp_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrsecnonctp_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureDelta Weighted".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP PnLup".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP PnLdown".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CVRup".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CVRdown".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbPlus Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_plus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbMinus Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_minus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Kb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Sb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_sb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbPlus Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_plus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbMinus Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_minus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Kb Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Sb Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_sb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbPlus High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_plus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature KbMinus High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_minus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Kb High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP Curvature Sb High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_sb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
    ]
}
