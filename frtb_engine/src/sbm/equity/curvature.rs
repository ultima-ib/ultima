#![allow(clippy::type_complexity)]

use crate::prelude::*;
use ndarray::Array2;
use ultibi::polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput};
use ultibi::prelude::CPM;
use ultibi::{BaseMeasure, IntoLazy};

pub fn eq_curv_delta(_: &CPM) -> PolarsResult<Expr> {
    Ok(curv_delta_spot("Equity"))
}
/// Helper functions
pub fn eq_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    eq_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeight"))
}
pub fn eq_cvr_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_spot("Equity", Cvr::Down))
}
pub fn eq_cvr_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr_spot("Equity", Cvr::Up))
}
pub fn eq_pnl_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "Equity", col("PnL_Up")))
}
pub fn eq_pnl_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "Equity", col("PnL_Down")))
}

pub(crate) fn eq_curvature_kb_plus_low(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_low(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_low(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_low(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn eq_curvature_kb_plus_medium(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_medium(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_medium(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn eq_curvature_kb_plus_high(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_high(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_high(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_high(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    eq_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn eq_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();

    let eq_curv_gamma = get_optional_parameter_array(
        op,
        format!("eq_curv_gamma{_suffix}").as_str(),
        &scenario.eq_curv_gamma,
    )?;
    let eq_curv_rho = get_optional_parameter(
        op,
        format!("eq_curv_diff_name_rho_per_bucket{_suffix}").as_str(),
        &scenario.eq_curv_diff_name_rho_per_bucket,
    )?;
    Ok(eq_curvature_charge(
        eq_curv_rho.to_vec(),
        eq_curv_gamma,
        rtrn,
        "Equity",
        Some(11),
    ))
}

pub(crate) fn eq_curvature_charge(
    eq_curv_rho: Vec<f64>,
    eq_curv_gamma: Array2<f64>,
    return_metric: ReturnMetric,
    rc: &'static str,
    special_bucket: Option<usize>,
) -> Expr {
    apply_multiple(
        move |columns| {
            let df = df![
                "rc"       => &columns[0],
                "b"        => &columns[1],
                "rf"       => &columns[2],
                "PnL_Up"   => &columns[3],
                "PnL_Down" => &columns[4],
                "SensitivitySpot"           => &columns[5],
                "CurvatureRiskWeight"       => &columns[6],
            ]?;

            let df = df
                .lazy()
                .filter(
                    col("rc").eq(lit(rc)).and(
                        col("PnL_Up")
                            .is_not_null()
                            .or(col("PnL_Down").is_not_null()),
                    ),
                )
                .groupby([col("b"), col("rf")])
                .agg([
                    cvr_up_spot().sum().alias("cvr_up"),
                    cvr_down_spot().sum().alias("cvr_down"),
                ])
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };
            let (kb_plus_cvr_up, kb_minus_cvr_down): (Vec<(f64, f64)>, Vec<(f64, f64)>) =
                curvature_kb_plus_minus(df, &eq_curv_rho, special_bucket)?;
            let (kb_plus, cvr_up): (Vec<f64>, Vec<f64>) = kb_plus_cvr_up.into_iter().unzip();
            let (kb_minus, cvr_down): (Vec<f64>, Vec<f64>) = kb_minus_cvr_down.into_iter().unzip();

            match return_metric {
                ReturnMetric::KbPlus => {
                    return Ok(Some(Series::new("res", [kb_plus.iter().sum::<f64>()])))
                }
                ReturnMetric::KbMinus => {
                    return Ok(Some(Series::new("res", [kb_minus.iter().sum::<f64>()])))
                }
                _ => (),
            }

            // If we want to reuse [kbs_sbs_curvature] the iterator has to be over Option<f64>
            let a = Some;
            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs_curvature(
                kb_plus,
                kb_minus,
                cvr_up.into_iter().map(a),
                cvr_down.into_iter().map(a),
            )?;
            match return_metric {
                ReturnMetric::Kb => return Ok(Some(Series::new("kbs", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("sbs", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }

            let phi = phi(&sbs);
            let gamma = phi * eq_curv_gamma.view();

            across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::Curvature)
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("RiskFactor"),
            col("PnL_Up"),
            col("PnL_Down"),
            col("SensitivitySpot"),
            col("CurvatureRiskWeight"),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn eq_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        eq_curvature_charge_low(op)?,
        eq_curvature_charge_medium(op)?,
        eq_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn eq_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(eq_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureDelta_Weighted".to_string(),
            calculator: std::sync::Arc::new(eq_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ PnLup".to_string(),
            calculator: std::sync::Arc::new(eq_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ PnLdown".to_string(),
            calculator: std::sync::Arc::new(eq_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CVRup".to_string(),
            calculator: std::sync::Arc::new(eq_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CVRdown".to_string(),
            calculator: std::sync::Arc::new(eq_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbPlus Medium".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_plus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbMinus Medium".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_minus_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Kb Medium".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Sb Medium".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_sb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbPlus Low".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_plus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbMinus Low".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_minus_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Kb Low".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Sb Low".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_sb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbPlus High".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_plus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature KbMinus High".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_minus_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Kb High".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ Curvature Sb High".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_sb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(eq_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(eq_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
    ]
}
