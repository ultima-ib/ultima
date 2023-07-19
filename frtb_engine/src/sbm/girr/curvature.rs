use crate::{
    prelude::*,
    sbm::common::{across_bucket_agg, rc_rcat_sens, SBMChargeType},
};
use ndarray::{Array1, Array2};
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput},
    BaseMeasure, IntoLazy, CPM,
};

#[cfg(feature = "CRR2")]
use super::delta::build_girr_crr2_gamma;
use crate::helpers::ReturnMetric;

pub fn ir_curv_delta(_: &CPM) -> PolarsResult<Expr> {
    Ok(curv_delta_total("GIRR"))
}

/// Helper functions
pub fn girr_curv_delta_weighted(op: &CPM) -> PolarsResult<Expr> {
    ir_curv_delta(op).map(|expr| expr * col("CurvatureRiskWeight"))
}

pub fn girr_cvr_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr("GIRR", Cvr::Down))
}

pub fn girr_cvr_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_cvr("GIRR", Cvr::Up))
}

pub fn girr_pnl_up(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "GIRR", col("PnL_Up")))
}

pub fn girr_pnl_down(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "GIRR", col("PnL_Down")))
}

// Kb, Sb, KbPlus, KbMinus is same across all scenarios for GIRR
pub(crate) fn girr_curvature_kb_plus(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)
}
pub(crate) fn girr_curvature_kb_minus(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)
}
pub(crate) fn girr_curvature_kb(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
pub(crate) fn girr_curvature_sb(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}

/// Calculate GIRR Curvature Capital charge
pub(crate) fn girr_curvature_charge_low(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
pub(crate) fn girr_curvature_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
pub(crate) fn girr_curvature_charge_high(op: &CPM) -> PolarsResult<Expr> {
    girr_curvature_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_curvature_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;
    let _suffix = scenario.as_str();

    let girr_curv_gamma = get_optional_parameter(
        op,
        format!("girr_curv_gamma{_suffix}").as_str(),
        &scenario.girr_curv_gamma,
    )?;
    let girr_vega_gamma_crr2_erm2 = get_optional_parameter(
        op,
        format!("girr_curv_gamma_erm2{_suffix}").as_str(),
        &scenario.girr_curv_gamma_erm2,
    )?;
    let erm2ccys = get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_ccys)?;

    Ok(girr_curvature_charge(
        girr_curv_gamma,
        girr_vega_gamma_crr2_erm2,
        rtrn,
        juri,
        erm2ccys,
    ))
}

/// https://www.clarusft.com/frtb-curvature-risk-charge/
/// Note: single Curvature Risk Charge (in Rates), ie CVR up/down, per currency
/// We therefore simply sum (no rho) CVR_Up/CVR_Down within a bucket
fn girr_curvature_charge(
    girr_curv_gamma: f64,
    _erm2_gamma: f64,
    return_metric: ReturnMetric,
    juri: Jurisdiction,
    _erm2ccys: Vec<String>,
) -> Expr {
    apply_multiple(
        move |columns| {
            let df = df![
                "rc"       => &columns[0],
                "b"        => &columns[1],
                "PnL_Up"   => &columns[2],
                "PnL_Down" => &columns[3],
                "SensitivitySpot" => &columns[4],
                "Sensitivity_025Y"=> &columns[5],
                "Sensitivity_05Y" => &columns[6],
                "Sensitivity_1Y"  => &columns[7],
                "Sensitivity_2Y"  => &columns[8],
                "Sensitivity_3Y"  => &columns[9],
                "Sensitivity_5Y"  => &columns[10],
                "Sensitivity_10Y" => &columns[11],
                "Sensitivity_15Y" => &columns[12],
                "Sensitivity_20Y" => &columns[13],
                "Sensitivity_30Y" => &columns[14],
                "CurvatureRiskWeight"=>&columns[15],
            ]?;

            let df = df
                .lazy()
                .filter(
                    col("rc").eq(lit("GIRR")).and(
                        col("PnL_Up")
                            .is_not_null()
                            .or(col("PnL_Down").is_not_null()),
                    ),
                )
                .groupby([col("b")])
                .agg([
                    cvr_up().sum().alias("cvr_up"),
                    cvr_down().sum().alias("cvr_down"),
                ])
                //.fill_null(lit::<f64>(0.))
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

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

            let _buckets: Vec<&str> = df["b"]
                .utf8()?
                .into_iter()
                .map(|s| s.unwrap_or("Default"))
                .collect();

            // 325ag
            let mut gamma = match juri {
                #[cfg(feature = "CRR2")]
                Jurisdiction::CRR2 => build_girr_crr2_gamma(
                    &_buckets,
                    &_erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                    girr_curv_gamma,
                    _erm2_gamma,
                ),
                _ => Array2::from_elem((kbs.len(), kbs.len()), girr_curv_gamma),
            };

            let phi = phi(&sbs);
            gamma = gamma * phi;

            let zeros = Array1::zeros(kbs.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::Curvature)
        },
        &[
            col("RiskClass"),
            col("BucketBCBS"),
            col("PnL_Up"),
            col("PnL_Down"),
            col("SensitivitySpot"),
            col("Sensitivity_025Y"),
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_2Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            col("Sensitivity_15Y"),
            col("Sensitivity_20Y"),
            col("Sensitivity_30Y"),
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
fn girr_curv_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        girr_curvature_charge_low(op)?,
        girr_curvature_charge_medium(op)?,
        girr_curvature_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn girr_curv_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureDelta".to_string(),
            calculator: std::sync::Arc::new(ir_curv_delta),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR PnLup".to_string(),
            calculator: std::sync::Arc::new(girr_pnl_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR PnLdown".to_string(),
            calculator: std::sync::Arc::new(girr_pnl_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureDelta Weighted".to_string(),
            calculator: std::sync::Arc::new(girr_curv_delta_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CVRup".to_string(),
            calculator: std::sync::Arc::new(girr_cvr_up),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CVRdown".to_string(),
            calculator: std::sync::Arc::new(girr_cvr_down),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR Curvature KbPlus".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_kb_plus),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR Curvature KbMinus".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_kb_minus),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR Curvature Kb".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_kb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR Curvature Sb".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureCharge Low".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureCharge Medium".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureCharge High".to_string(),
            calculator: std::sync::Arc::new(girr_curvature_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR CurvatureCharge MAX".to_string(),
            calculator: std::sync::Arc::new(girr_curv_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
    ]
}
