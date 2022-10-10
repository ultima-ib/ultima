#![allow(clippy::type_complexity)]

use crate::prelude::*;
use base_engine::prelude::OCP;
use ndarray::{Array1, Array2};
//use ndarray::{Array1, Array2};
use polars::prelude::*;

pub fn eq_curv_delta(_: &OCP) -> Expr {
    curv_delta_spot("Equity")
}
/// Helper functions
pub fn eq_curv_delta_weighted(op: &OCP) -> Expr {
    eq_curv_delta(op) * col("CurvatureRiskWeight")
}
pub fn eq_cvr_down(_: &OCP) -> Expr {
    rc_cvr_spot("Equity", CVR::Down)
}
pub fn eq_cvr_up(_: &OCP) -> Expr {
    rc_cvr_spot("Equity", CVR::Up)
}
pub fn eq_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Equity", col("PnL_Up"))
}
pub fn eq_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Equity", col("PnL_Down"))
}

pub(crate) fn eq_curvature_kb_plus_low(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_low(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_low(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_low(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_low(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn eq_curvature_kb_plus_medium(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_medium(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_medium(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_medium(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_medium(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn eq_curvature_kb_plus_high(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn eq_curvature_kb_minus_high(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn eq_curvature_kb_high(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn eq_curvature_sb_high(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn eq_curvature_charge_high(op: &OCP) -> Expr {
    eq_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn eq_curvature_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();

    let eq_curv_gamma = get_optional_parameter_array(
        op,
        format!("eq_curv_gamma{_suffix}").as_str(),
        &scenario.eq_curv_gamma,
    );
    let eq_curv_rho = get_optional_parameter(
        op,
        format!("eq_curv_diff_name_rho_per_bucket{_suffix}").as_str(),
        &scenario.eq_curv_diff_name_rho_per_bucket,
    );
    eq_curvature_charge(
        eq_curv_rho.to_vec(),
        eq_curv_gamma,
        rtrn,
        "Equity",
        Some(11),
    )
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
                "rc"       => columns[0].clone(),
                "b"        => columns[1].clone(),
                "rf"       => columns[2].clone(),
                "PnL_Up"   => columns[3].clone(),
                "PnL_Down" => columns[4].clone(),
                "SensitivitySpot"           => columns[5].clone(),
                "CurvatureRiskWeight"       => columns[6].clone(),
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

            let res_len = columns[0].len();
            if df.height() == 0 {
                return Ok(Series::from_vec(
                    "res",
                    vec![0.; columns[0].len()] as Vec<f64>,
                ));
            };
            let (kb_plus_cvr_up, kb_minus_cvr_down): (Vec<(f64, f64)>, Vec<(f64, f64)>) =
                curvature_kb_plus_minus(df, &eq_curv_rho, special_bucket)?;
            let (kb_plus, cvr_up): (Vec<f64>, Vec<f64>) = kb_plus_cvr_up.into_iter().unzip();
            let (kb_minus, cvr_down): (Vec<f64>, Vec<f64>) = kb_minus_cvr_down.into_iter().unzip();

            match return_metric {
                ReturnMetric::KbPlus => {
                    return Ok(Series::new(
                        "res",
                        Array1::<f64>::from_elem(res_len, kb_plus.iter().sum())
                            .as_slice()
                            .unwrap(),
                    ))
                }
                ReturnMetric::KbMinus => {
                    return Ok(Series::new(
                        "res",
                        Array1::<f64>::from_elem(res_len, kb_minus.iter().sum())
                            .as_slice()
                            .unwrap(),
                    ))
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
                ReturnMetric::Kb => {
                    return Ok(Series::new(
                        "res",
                        Array1::<f64>::from_elem(res_len, kbs.iter().sum())
                            .as_slice()
                            .unwrap(),
                    ))
                }
                ReturnMetric::Sb => {
                    return Ok(Series::new(
                        "res",
                        Array1::<f64>::from_elem(res_len, sbs.iter().sum())
                            .as_slice()
                            .unwrap(),
                    ))
                }
                _ => (),
            }

            let phi = phi(&sbs);
            let gamma = phi * eq_curv_gamma.view();

            across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::Curvature)
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
    )
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn eq_curv_max(op: &OCP) -> Expr {
    max_exprs(&[
        eq_curvature_charge_low(op),
        eq_curvature_charge_medium(op),
        eq_curvature_charge_high(op),
    ])
}

/// Exporting Measures
pub(crate) fn eq_curv_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "EQ_CurvatureDelta".to_string(),
            calculator: Box::new(eq_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CurvatureDelta_Weighted".to_string(),
            calculator: Box::new(eq_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_PnLup".to_string(),
            calculator: Box::new(eq_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_PnLdown".to_string(),
            calculator: Box::new(eq_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CVRup".to_string(),
            calculator: Box::new(eq_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CVRdown".to_string(),
            calculator: Box::new(eq_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbPlus_Medium".to_string(),
            calculator: Box::new(eq_curvature_kb_plus_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbMinus_Medium".to_string(),
            calculator: Box::new(eq_curvature_kb_minus_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Kb_Medium".to_string(),
            calculator: Box::new(eq_curvature_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Sb_Medium".to_string(),
            calculator: Box::new(eq_curvature_sb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CurvatureCharge_Medium".to_string(),
            calculator: Box::new(eq_curvature_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbPlus_Low".to_string(),
            calculator: Box::new(eq_curvature_kb_plus_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbMinus_Low".to_string(),
            calculator: Box::new(eq_curvature_kb_minus_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Kb_Low".to_string(),
            calculator: Box::new(eq_curvature_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Sb_Low".to_string(),
            calculator: Box::new(eq_curvature_sb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CurvatureCharge_Low".to_string(),
            calculator: Box::new(eq_curvature_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbPlus_High".to_string(),
            calculator: Box::new(eq_curvature_kb_plus_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_KbMinus_High".to_string(),
            calculator: Box::new(eq_curvature_kb_minus_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Kb_High".to_string(),
            calculator: Box::new(eq_curvature_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_Curvature_Sb_High".to_string(),
            calculator: Box::new(eq_curvature_sb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CurvatureCharge_High".to_string(),
            calculator: Box::new(eq_curvature_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
        Measure {
            name: "EQ_CurvatureCharge_MAX".to_string(),
            calculator: Box::new(eq_curv_max),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
        },
    ]
}
