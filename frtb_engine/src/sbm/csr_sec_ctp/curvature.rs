use crate::{prelude::*, sbm::csr_nonsec::curvature::csrnonsec_curvature_charge};
use polars::prelude::*;
use ultibi::{prelude::CPM, BaseMeasure};

pub fn csrsecctp_curv_delta(_: &CPM) -> PolarsResult<Expr> {
    Ok(curv_delta_5("CSR_Sec_CTP"))
}
/// Helper functions
pub fn csrsecctp_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;
    match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => {
            csrsecctp_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeightCRR2"))
        }
        Jurisdiction::BCBS => {
            csrsecctp_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeight"))
        }
    }
}

pub fn csrsecctp_cvr_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_5("CSR_Sec_CTP", Cvr::Down))
}
pub fn csrsecctp_cvr_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_5("CSR_Sec_CTP", Cvr::Up))
}
pub fn csrsecctp_pnl_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_Sec_CTP", col("PnL_Up")))
}
pub fn csrsecctp_pnl_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_Sec_CTP", col("PnL_Down")))
}

pub(crate) fn csrsecctp_curvature_kb_plus_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csrsecctp_curvature_kb_minus_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csrsecctp_curvature_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csrsecctp_curvature_sb_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csrsecctp_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csrsecctp_curvature_kb_plus_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csrsecctp_curvature_kb_minus_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csrsecctp_curvature_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csrsecctp_curvature_sb_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csrsecctp_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csrsecctp_curvature_kb_plus_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn csrsecctp_curvature_kb_minus_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn csrsecctp_curvature_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn csrsecctp_curvature_sb_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csrsecctp_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn csrsecctp_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();
    let juri: Jurisdiction = get_jurisdiction(op)?;

    let (weight, bucket_col, name_rho_vec, gamma, special_bucket) = match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (
            col("CurvatureRiskWeightCRR2"),
            col("BucketCRR2"),
            Vec::from(scenario.csr_ctp_curv_diff_name_rho_per_bucket_crr2),
            &scenario.csr_ctp_curv_gamma_crr2,
            None,
        ),

        Jurisdiction::BCBS => (
            col("CurvatureRiskWeight"),
            col("BucketBCBS"),
            Vec::from(scenario.csr_ctp_curv_diff_name_rho_per_bucket_bcbs),
            &scenario.csr_ctp_curv_gamma_bcbs,
            None,
        ),
    };

    let csr_secctp_curv_gamma =
        get_optional_parameter_array(op, format!("csr_ctp_curv_gamma{_suffix}").as_str(), gamma)?;
    let csr_secctp_curv_rho = get_optional_parameter_vec(
        op,
        format!("csr_ctp_curv_diff_name_rho_per_bucket{_suffix}").as_str(),
        &name_rho_vec,
    )?;

    Ok(csrnonsec_curvature_charge(
        csr_secctp_curv_rho,
        csr_secctp_curv_gamma,
        rtrn,
        special_bucket,
        weight,
        bucket_col,
        "CSR_Sec_CTP",
    ))
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecctp_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csrsecctp_curvature_charge_low(op)?,
        csrsecctp_curvature_charge_medium(op)?,
        csrsecctp_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrsecctp_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureDelta_Weighted".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP PnLup".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP PnLdown".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CVRup".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CVRdown".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbPlus Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_plus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbMinus Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_minus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Kb Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Sb Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_sb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbPlus Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_plus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbMinus Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_minus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Kb Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Sb Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_sb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbPlus High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_plus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature KbMinus High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_minus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Kb High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP Curvature Sb High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_sb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
    ]
}
