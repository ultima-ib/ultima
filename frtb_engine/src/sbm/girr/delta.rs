use crate::prelude::*;
use ultibi::{
    polars::prelude::{
        apply_multiple, col, df, lit, max_horizontal, when, DataType, GetOutput, PolarsError,
    },
    BaseMeasure, IntoLazy, CPM,
};

//use polars::lazy::dsl::{apply_multiple, col, lit, when};
use rayon::prelude::IntoParallelIterator;

use crate::risk_weights::REDUCED_IR_WEIGHT;
use ndarray::{parallel::prelude::ParallelIterator, Array1, Array2};

pub fn total_ir_delta_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "GIRR", total_delta_sens()))
}
/// Helper functions
fn girr_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "SensitivitySpot", "SensWeights", 0)
}
fn girr_delta_sens_weighted_025y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_025Y", "SensWeights", 1)
}
fn girr_delta_sens_weighted_05y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_05Y", "SensWeights", 2)
}
fn girr_delta_sens_weighted_1y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_1Y", "SensWeights", 3)
}
fn girr_delta_sens_weighted_2y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_2Y", "SensWeights", 4)
}
fn girr_delta_sens_weighted_3y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_3Y", "SensWeights", 5)
}
fn girr_delta_sens_weighted_5y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_5Y", "SensWeights", 6)
}
fn girr_delta_sens_weighted_10y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_10Y", "SensWeights", 7)
}
fn girr_delta_sens_weighted_15y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_15Y", "SensWeights", 8)
}
fn girr_delta_sens_weighted_20y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_20Y", "SensWeights", 9)
}
fn girr_delta_sens_weighted_30y() -> Expr {
    rc_tenor_weighted_sens("Delta", "GIRR", "Sensitivity_30Y", "SensWeights", 10)
}

/// Total GIRR Delta Seins
pub(crate) fn girr_delta_sens_weighted(_: &CPM) -> PolarsResult<Expr> {
    Ok(girr_delta_sens_weighted_spot().fill_null(0.)
        + girr_delta_sens_weighted_025y().fill_null(0.)
        + girr_delta_sens_weighted_05y().fill_null(0.)
        + girr_delta_sens_weighted_1y().fill_null(0.)
        + girr_delta_sens_weighted_2y().fill_null(0.)
        + girr_delta_sens_weighted_3y().fill_null(0.)
        + girr_delta_sens_weighted_5y().fill_null(0.)
        + girr_delta_sens_weighted_10y().fill_null(0.)
        + girr_delta_sens_weighted_15y().fill_null(0.)
        + girr_delta_sens_weighted_20y().fill_null(0.)
        + girr_delta_sens_weighted_30y().fill_null(0.))
}

/// Interm Result: GIRR Delta Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn girr_delta_sb(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

///calculate GIRR Delta Low Capital charge
pub(crate) fn girr_delta_charge_low(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
/// Interm Result: GIRR Delta Kb Low
pub(crate) fn girr_delta_kb_low(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate GIRR Delta Medium Capital charge
pub(crate) fn girr_delta_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
/// Interm Result: GIRR Delta Kb Medium
pub(crate) fn girr_delta_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate GIRR Delta High Capital charge
pub(crate) fn girr_delta_charge_high(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
/// Interm Result: GIRR Delta Kb High
pub(crate) fn girr_delta_kb_high(op: &CPM) -> PolarsResult<Expr> {
    girr_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_delta_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;

    let rep_ccy = if let Some(s) = op.get("reporting_ccy") {
        if s.len() == 3 {
            Ok(s.to_owned())
        } else {
            Err(PolarsError::ComputeError(
                "reporting_ccy must be of length 3: CCY".into(),
            ))
        }
    } else {
        match juri {
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => Ok("EUR".to_string()),
            _ => Ok("USD".to_string()),
        }
    }?;

    let _suffix = scenario.as_str();

    // Take MEDIUM scenario here because scenario_fn is to be applied post factum
    let girr_delta_rho_same_curve = get_optional_parameter_array(
        op,
        "girr_delta_rho_same_curve_base",
        &MEDIUM_CORR_SCENARIO.girr_delta_rho_same_curve_base,
    )?;
    let girr_delta_rho_diff_curve = get_optional_parameter(
        op,
        "girr_delta_rho_diff_curve_base",
        &MEDIUM_CORR_SCENARIO.girr_delta_rho_diff_curve_base,
    )?;
    let girr_delta_rho_infl = get_optional_parameter(
        op,
        "girr_delta_rho_infl_base",
        &MEDIUM_CORR_SCENARIO.girr_delta_rho_infl_base,
    )?;
    let girr_delta_rho_xccy = get_optional_parameter(
        op,
        "girr_delta_rho_xccy_base",
        &MEDIUM_CORR_SCENARIO.girr_delta_rho_xccy_base,
    )?;

    let girr_delta_gamma = get_optional_parameter(
        op,
        format!("girr_delta_gamma{_suffix}").as_str(),
        &scenario.girr_delta_vega_gamma,
    )?;
    let girr_delta_gamma_crr2_erm2 = get_optional_parameter(
        op,
        format!("girr_delta_gamma_erm2{_suffix}").as_str(),
        &scenario.girr_delta_vega_gamma_erm2,
    )?;
    let erm2ccys = get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_ccys)?;

    Ok(girr_delta_charge(
        girr_delta_gamma,
        girr_delta_rho_same_curve,
        girr_delta_rho_diff_curve,
        girr_delta_rho_infl,
        girr_delta_rho_xccy,
        rtrn,
        juri,
        girr_delta_gamma_crr2_erm2,
        erm2ccys,
        scenario.scenario_fn,
        rep_ccy,
    ))
}

#[allow(clippy::too_many_arguments)]
fn girr_delta_charge<F>(
    girr_delta_gamma: f64,
    girr_delta_rho_same_curve: Array2<f64>,
    girr_delta_rho_diff_curve: f64,
    girr_delta_rho_infl: f64,
    girr_delta_rho_xccy: f64,
    return_metric: ReturnMetric,
    juri: Jurisdiction,
    _erm2_gamma: f64,
    _erm2ccys: Vec<String>,
    scenario_fn: F,
    rep_ccy: String,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    apply_multiple(
        move |columns| {
            let mut df = df![
                "rcat" => columns[15].clone(),
                "rc" =>   columns[0].clone(),
                "rf" =>   columns[1].clone(),
                "rft" =>  columns[2].clone(),
                "b" =>    columns[3].clone(),
                "y0" =>   columns[4].clone(),
                "y025" => columns[5].clone(),
                "y05" =>  columns[6].clone(),
                "y1" =>   columns[7].clone(),
                "y2" =>   columns[8].clone(),
                "y3" =>   columns[9].clone(),
                "y5" =>   columns[10].clone(),
                "y10" =>  columns[11].clone(),
                "y15" =>  columns[12].clone(),
                "y20" =>  columns[13].clone(),
                "y30" =>  columns[14].clone(),

                "w0" =>   columns[16].clone(),
                "w025" => columns[17].clone(),
                "w05" =>  columns[18].clone(),
                "w1" =>   columns[19].clone(),
                "w2" =>   columns[20].clone(),
                "w3" =>   columns[21].clone(),
                "w5" =>   columns[22].clone(),
                "w10" =>  columns[23].clone(),
                "w15" =>  columns[24].clone(),
                "w20" =>  columns[25].clone(),
                "w30" =>  columns[26].clone(),
            ]?;

            // If the weight of the reporting currency has not yet been reduced - reduce it
            if !REDUCED_IR_WEIGHT.contains(rep_ccy.as_str()) {
                df = df
                    .lazy()
                    .with_columns([
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w0") / lit(2_f64.sqrt()))
                            .otherwise(col("w0")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w025") / lit(2_f64.sqrt()))
                            .otherwise(col("w025")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w05") / lit(2_f64.sqrt()))
                            .otherwise(col("w05")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w1") / lit(2_f64.sqrt()))
                            .otherwise(col("w1")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w2") / lit(2_f64.sqrt()))
                            .otherwise(col("w2")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w3") / lit(2_f64.sqrt()))
                            .otherwise(col("w3")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w5") / lit(2_f64.sqrt()))
                            .otherwise(col("w5")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w10") / lit(2_f64.sqrt()))
                            .otherwise(col("w10")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w15") / lit(2_f64.sqrt()))
                            .otherwise(col("w15")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w20") / lit(2_f64.sqrt()))
                            .otherwise(col("w20")),
                        when(col("b").eq(lit(rep_ccy.as_str())))
                            .then(col("w30") / lit(2_f64.sqrt()))
                            .otherwise(col("w30")),
                    ])
                    .collect()?
            }

            df = df
                .lazy()
                .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Delta"))))
                .groupby([col("b"), col("rf"), col("rft")])
                .agg([
                    (col("y0") * col("w0")).sum(),
                    (col("y025") * col("w025")).sum(),
                    (col("y05") * col("w05")).sum(),
                    (col("y1") * col("w1")).sum(),
                    (col("y2") * col("w2")).sum(),
                    (col("y3") * col("w3")).sum(),
                    (col("y5") * col("w5")).sum(),
                    (col("y10") * col("w10")).sum(),
                    (col("y15") * col("w15")).sum(),
                    (col("y20") * col("w20")).sum(),
                    (col("y30") * col("w30")).sum(),
                ])
                .fill_null(lit::<f64>(0.))
                .with_columns([
                    when(col("rft").eq(lit("XCCY")))
                        .then(col("y0"))
                        .otherwise(NULL.lit())
                        .alias("XCCY"),
                    when(col("rft").eq(lit("Inflation")))
                        .then(col("y0"))
                        .otherwise(NULL.lit())
                        .alias("Inflation"),
                ])
                .select([col("*").exclude(["rft"])])
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let part = df.partition_by(["b"], true)?;

            let res_buckets_kbs_sbs: PolarsResult<Vec<(String, (f64, f64))>> = part
                .into_par_iter()
                .map(|bdf| {
                    bucket_kb_sb_single_type(
                        &bdf,
                        &girr_delta_rho_same_curve,
                        girr_delta_rho_diff_curve,
                        scenario_fn,
                        &[
                            "y025", "y05", "y1", "y2", "y3", "y5", "y10", "y15", "y20", "y30",
                        ],
                        Some((girr_delta_rho_infl, girr_delta_rho_xccy)),
                        None,
                    )
                })
                .collect();

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let (_buckets, (kbs, sbs)): (Vec<String>, (Vec<f64>, Vec<f64>)) =
                res_buckets_kbs_sbs?.into_iter().unzip();

            // Early return Kb or Sb is that is the required metric
            match return_metric {
                ReturnMetric::Kb => return Ok(Some(Series::new("res", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("res", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }
            // Need to differentiate between CRR2 and BCBS
            // 325ag
            let mut gamma = match juri {
                #[cfg(feature = "CRR2")]
                Jurisdiction::CRR2 => {
                    let _buckets_str: Vec<&str> = _buckets.iter().map(|s| &**s).collect();
                    build_girr_crr2_gamma(
                        &_buckets_str,
                        &_erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                        girr_delta_gamma,
                        _erm2_gamma,
                    )
                }
                _ => Array2::from_elem((kbs.len(), kbs.len()), girr_delta_gamma),
            };

            let zeros = Array1::zeros(kbs.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
        },
        &[
            col("RiskClass"),
            col("RiskFactor"),
            col("RiskFactorType"),
            col("BucketBCBS"),
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
            col("RiskCategory"),
            //col("SensWeights"),
            col("SensWeights").list().get(lit(0)),
            col("SensWeights").list().get(lit(1)),
            col("SensWeights").list().get(lit(2)),
            col("SensWeights").list().get(lit(3)),
            col("SensWeights").list().get(lit(4)),
            col("SensWeights").list().get(lit(5)),
            col("SensWeights").list().get(lit(6)),
            col("SensWeights").list().get(lit(7)),
            col("SensWeights").list().get(lit(8)),
            col("SensWeights").list().get(lit(9)),
            col("SensWeights").list().get(lit(10)),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

/// 325ag
#[cfg(feature = "CRR2")]
#[allow(clippy::if_same_then_else)]
pub(crate) fn build_girr_crr2_gamma(
    buckets: &[&str],
    erm2ccys: &[&str],
    base_gamma: f64,
    erm2vseur: f64,
) -> Array2<f64> {
    use ndarray::Axis;

    let mut gamma = Array2::from_elem((buckets.len(), buckets.len()), base_gamma);

    gamma
        .axis_iter_mut(Axis(0)) // Iterate over rows
        .enumerate()
        .for_each(|(i, mut row)| {
            let buck1 = unsafe { buckets.get_unchecked(i) };
            // if bucket 1 is ERM2 or EUR (NOTE: this rule applies to CRR2 only, and therefore to EUR only)
            if erm2ccys.contains(buck1) | (*buck1 == "EUR") {
                row.indexed_iter_mut().for_each(|(j, x)| {
                    let buck2 = unsafe { buckets.get_unchecked(j) };
                    if ((*buck1 == "EUR") & erm2ccys.contains(buck2))
                        | (erm2ccys.contains(buck1) & (*buck2 == "EUR"))
                    {
                        // if  EUR vs ERM2
                        // or ERM2 vs EUR
                        *x = erm2vseur;
                    }
                })
            }
        });
    gamma
}

/// 21.46 GIRR delta risk correlation within same bucket, same curve
/// results in 10x10 matrix
/// Used at the initiation of the program using OnceCell
pub(crate) fn girr_corr_matrix() -> Array2<f64> {
    let theta: f64 = -0.03;
    let mut base_weights = Array2::<f64>::zeros((10, 10));
    let tenors = [0.25, 0.5, 1., 2., 3., 5., 10., 15., 20., 30.];

    for ((row, col), val) in base_weights.indexed_iter_mut() {
        let tr = tenors[row];
        let tc = tenors[col];
        *val = f64::max(f64::exp(theta * f64::abs(tr - tc) / tr.min(tc)), 0.4);
    }
    base_weights
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn girr_delta_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        girr_delta_charge_low(op)?,
        girr_delta_charge_medium(op)?,
        girr_delta_charge_high(op)?,
    ]))
}
/// Exporting Measures
pub(crate) fn girr_delta_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaSens".to_string(),
            calculator: std::sync::Arc::new(total_ir_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(girr_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaSb".to_string(),
            calculator: std::sync::Arc::new(girr_delta_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaCharge Low".to_string(),
            calculator: std::sync::Arc::new(girr_delta_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaKb Low".to_string(),
            calculator: std::sync::Arc::new(girr_delta_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(girr_delta_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaKb Medium".to_string(),
            calculator: std::sync::Arc::new(girr_delta_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaCharge High".to_string(),
            calculator: std::sync::Arc::new(girr_delta_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaKb High".to_string(),
            calculator: std::sync::Arc::new(girr_delta_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "GIRR DeltaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(girr_delta_max),
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
