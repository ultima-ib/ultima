use crate::prelude::*;
use ndarray::{Array1, Array2};
use ultibi::{
    polars::prelude::{
        apply_multiple, df, max_horizontal, ChunkFillNullValue, ChunkSet, DataType, Float64Chunked,
        GetOutput, IntoSeries, NumOpsDispatch, Utf8NameSpaceImpl,
    },
    BaseMeasure, IntoLazy, CPM,
};

use super::delta::ccy_regex;

/// Wrapper used for FX only. Filteres where BucketBCBS is of
/// th form ...CCY where CCY is the reporting CCY
fn risk_filtered_by_ccy(op: &CPM, risk: Expr) -> PolarsResult<Expr> {
    let ccy_regex = ccy_regex(op)?;
    Ok(risk.apply_many(
        move |columns| {
            let mask = columns[1].utf8()?.contains(ccy_regex.as_str(), false)?;

            let res = columns[0].f64()?.set(&!mask, None)?;

            Ok(Some(res.into_series()))
        },
        &[col("BucketBCBS")],
        GetOutput::from_type(DataType::Float64),
    ))
}
/// FX Curvature Delta, filtered by reporting ccy
pub fn fx_curv_delta(op: &CPM) -> PolarsResult<Expr> {
    risk_filtered_by_ccy(op, curv_delta_spot("FX"))
}
// FX CurvatureDelta Weighted, filtered by reporting ccy
pub fn fx_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    Ok(risk_filtered_by_ccy(op, curv_delta_spot("FX"))? * col("CurvatureRiskWeight"))
}
/// FX PnL Up, filtered by reporting ccy
pub fn fx_pnl_up(op: &CPM) -> PolarsResult<Expr> {
    risk_filtered_by_ccy(op, rc_rcat_sens("Delta", "FX", col("PnL_Up")))
}
/// FX PnL Down, filtered by reporting ccy
pub fn fx_pnl_down(op: &CPM) -> PolarsResult<Expr> {
    risk_filtered_by_ccy(op, rc_rcat_sens("Delta", "FX", col("PnL_Down")))
}

/// FX CVR Divide by 1.5
/// as per 21.98
/// column FxCurvDivEligibility must be present
fn fx_cvr_up_down(div: bool, risk: Expr) -> Expr {
    if !div {
        risk
    } else {
        apply_multiple(
            |columns| {
                let mult: Vec<f64> = vec![1.; columns[0].len()];
                let mult = Float64Chunked::from_vec("multiplicator", mult);
                let mask = columns[1].bool()?.fill_null_with_values(false)?;
                let mult = mult.set(&mask, Some(1.5))?.into_series();
                let div = columns[0].f64()?.divide(&mult)?;
                Ok(Some(div))
            },
            &[risk, col("FxCurvDivEligibility")],
            GetOutput::from_type(DataType::Float64),
            false,
        )
    }
}

pub fn fx_cvr_up(op: &CPM) -> PolarsResult<Expr> {
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false)?;
    let risk = risk_filtered_by_ccy(op, rc_cvr_spot("FX", Cvr::Up))?;
    Ok(fx_cvr_up_down(div, risk))
}
pub fn fx_cvr_down(op: &CPM) -> PolarsResult<Expr> {
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false)?;
    let risk = risk_filtered_by_ccy(op, rc_cvr_spot("FX", Cvr::Down))?;
    Ok(fx_cvr_up_down(div, risk))
}

// Kb, Sb, KbPlus, KbMinus is same across all scenarios for FX
pub(crate) fn fx_curvature_kb_plus(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn fx_curvature_kb_minus(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn fx_curvature_kb(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn fx_curvature_sb(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}

/// Calculate FX Curvature Capital charge
pub(crate) fn fx_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
pub(crate) fn fx_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
pub(crate) fn fx_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    fx_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn fx_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let ccy_regex = ccy_regex(op)?;

    let _suffix = scenario.as_str();
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false)?;

    let fx_curv_gamma = get_optional_parameter(
        op,
        format!("fx_curv_gamma{_suffix}").as_str(),
        &scenario.fx_curv_gamma,
    )?;

    Ok(fx_curvature_charge(fx_curv_gamma, rtrn, ccy_regex, div))
}

fn fx_curvature_charge(
    gamma: f64,
    return_metric: ReturnMetric,
    ccy_regex: String,
    div: bool,
) -> Expr {
    apply_multiple(
        move |columns| {
            let df = df![
                "rc"       => &columns[0],
                "b"        => &columns[1],
                "PnL_Up"   => &columns[2],
                "PnL_Down" => &columns[3],
                "SensitivitySpot" =>    &columns[4],
                "CurvatureRiskWeight"=> &columns[5],
                "FxCurvDivEligibility"=>&columns[6],
            ]?;

            //dbg!(&df);

            let ccy_regex = ccy_regex.clone();
            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit("FX"))
                        .and(col("CurvatureRiskWeight").is_not_null())
                        .and(col("b").apply(
                            move |col| {
                                Ok(Some(col.utf8()?.contains(&ccy_regex, false)?.into_series()))
                            },
                            GetOutput::from_type(DataType::Boolean),
                        )),
                )
                .collect()?;

            let res_len = columns[0].len();

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let df = df
                .lazy()
                .groupby([col("b")])
                .agg([
                    fx_cvr_up_down(div, cvr_up_spot()).sum().alias("cvr_up"),
                    fx_cvr_up_down(div, cvr_down_spot()).sum().alias("cvr_down"),
                ])
                .collect()?;

            let kb_plus: Vec<f64> = kb_plus_minus_simple(&df["cvr_up"])?;
            if let ReturnMetric::KbPlus = return_metric {
                return Ok(Some(Series::new("res", [kb_plus.iter().sum::<f64>()])));
            }

            let kb_minus: Vec<f64> = kb_plus_minus_simple(&df["cvr_down"])?;
            if let ReturnMetric::KbMinus = return_metric {
                return Ok(Some(Series::new("res", [kb_minus.iter().sum::<f64>()])));
            }

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs_curvature(
                kb_plus,
                kb_minus,
                df["cvr_up"].f64()?.into_iter(),
                df["cvr_down"].f64()?.into_iter(),
            )?;
            match return_metric {
                ReturnMetric::Kb => return Ok(Some(Series::new("res", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("res", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }

            // 325ag
            let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), gamma);
            let phi = phi(&sbs);
            gamma = gamma * phi;

            let zeros = Array1::zeros(kbs.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::Curvature)
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("PnL_Up"),
            col("PnL_Down"),
            col("SensitivitySpot"),
            col("CurvatureRiskWeight"),
            col("FxCurvDivEligibility"),
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
fn fx_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        fx_curvature_charge_low(op)?,
        fx_curvature_charge_medium(op)?,
        fx_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn fx_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "FX CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(fx_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CurvatureDelta Weighted".to_string(),
            calculator: std::sync::Arc::new(fx_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX PnLup".to_string(),
            calculator: std::sync::Arc::new(fx_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX PnLdown".to_string(),
            calculator: std::sync::Arc::new(fx_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CVRup".to_string(),
            calculator: std::sync::Arc::new(fx_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CVRdown".to_string(),
            calculator: std::sync::Arc::new(fx_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX Curvature KbPlus".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_kb_plus),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX Curvature KbMinus".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_kb_minus),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX Curvature Kb".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_kb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX Curvature Sb".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(fx_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(fx_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
    ]
}
