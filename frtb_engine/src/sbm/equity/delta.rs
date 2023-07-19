//! Main Equity Delta Calculator
//! For construction of Rho Note:
//! We never have same type AND same issuer since these were netted
//! ie never APPspot APPspot
//! APPLspot APPLrepo is 0.999*1 because spot != repo(0.999), and APP APP (1)
//! APPLspot GOOGspot/APPLrepo GOOGrepo
//! is 1*0.25 because spot == spot (1) and Goog != App (0.25)
//! Apprepo Googspot is 0.999*0.25 because repo != spot and App != Goog (0.25)
//! Hence, it's sufficient to build two matrixes:
//! 1 based on rft and 2 based on rf

use crate::prelude::*;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput},
    BaseMeasure, IntoLazy, CPM,
};

use ndarray::Array2;

/// Total Equity Delta Sens
pub(crate) fn equity_delta_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "Equity", col("SensitivitySpot")))
}
// wrapper of equity_delta_sens_weighted_spot which takes a param o
pub(crate) fn equity_delta_sens_weighted(_: &CPM) -> PolarsResult<Expr> {
    Ok(equity_delta_sens_weighted_spot())
}
///
pub(crate) fn equity_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens("Delta", "Equity", "SensitivitySpot", "SensWeights", 0)
}
/// Interm Result: Equity Delta Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn eq_delta_sb(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
/// Interm Result: Equity Kb Low
pub(crate) fn eq_delta_kb_low(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
/// Interm Result: Equity Kb Medium
pub(crate) fn eq_delta_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
/// Interm Result: Equity Kb High
pub(crate) fn eq_delta_kb_high(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Delta High Capital charge
pub(crate) fn equity_delta_charge_high(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate Equity Delta Medium Capital charge
pub(crate) fn equity_delta_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate Equity Delta Low Capital charge
pub(crate) fn equity_delta_charge_low(op: &CPM) -> PolarsResult<Expr> {
    equity_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn equity_delta_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();
    let eq_gamma = get_optional_parameter_array(
        op,
        format!("eq_delta_gamma{_suffix}").as_str(),
        &scenario.eq_delta_vega_gamma,
    )?;
    let base_eq_rho_bucket = get_optional_parameter(
        op,
        "eq_delta_diff_name_rho_per_bucket_base",
        &scenario.eq_delta_vega_diff_name_rho_per_bucket_base,
    )?;
    let eq_rho_diff_type = get_optional_parameter(
        op,
        "eq_delta_diff_type_rho_base",
        &scenario.eq_delta_diff_type_rho_base,
    )?;

    Ok(equity_delta_charge(
        eq_gamma,
        base_eq_rho_bucket,
        eq_rho_diff_type,
        scenario.scenario_fn,
        rtrn,
    ))
}

/// calculate FX Delta Capital charge
fn equity_delta_charge<F>(
    gamma: Array2<f64>,
    eq_rho_bucket: [f64; 13],
    eq_rho_diff_type: f64,
    scenario_fn: F,
    rtrn: ReturnMetric,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    // inner function
    apply_multiple(
        move |columns| {
            let mut df = df![
                "rcat" => &columns[0],
                "rc"   => &columns[1],
                "b"    => &columns[2],
                "rf"   => &columns[3],
                "rft"  => &columns[4],
                "d"    => &columns[5],
                "w"    => &columns[6],
            ]?;

            // 21.4.3 - Netting
            df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit("Equity"))
                        .and(col("rcat").eq(lit("Delta"))),
                )
                // TODO Fill empty bucket with 11 here
                .with_columns([
                    when(col("rft").eq(lit("EqSpot")))
                        .then((col("d") * col("w")).alias("Spot"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("EqRepo")))
                        .then((col("d") * col("w")).alias("Repo"))
                        .otherwise(NULL.lit()),
                ])
                .groupby([col("b"), col("rf")])
                .agg([col("Spot").sum(), col("Repo").sum()])
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            // 21.78
            let kbs_sbs = all_kbs_sbs_two_types(
                df,
                13,
                &eq_rho_bucket,
                eq_rho_diff_type,
                scenario_fn,
                Some(11),
                &[("Spot", "Repo")],
                None,
            )?;

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

            // Early return Kb or Sb is that is the required metric

            match rtrn {
                ReturnMetric::Kb => return Ok(Some(Series::new("kbs", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("sbs", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }

            across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
        },
        &[
            col("RiskCategory"),
            col("RiskClass"),
            col("BucketBCBS"),
            col("RiskFactor"),
            col("RiskFactorType"),
            col("SensitivitySpot"),
            col("SensWeights").list().get(lit(0)),
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
fn eq_delta_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        equity_delta_charge_low(op)?,
        equity_delta_charge_medium(op)?,
        equity_delta_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn eq_delta_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "EQ DeltaSens".to_string(),
            calculator: std::sync::Arc::new(equity_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(equity_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaSb".to_string(),
            calculator: std::sync::Arc::new(eq_delta_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaKb Low".to_string(),
            calculator: std::sync::Arc::new(eq_delta_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaKb Medium".to_string(),
            calculator: std::sync::Arc::new(eq_delta_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaKb High".to_string(),
            calculator: std::sync::Arc::new(eq_delta_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaCharge Low".to_string(),
            calculator: std::sync::Arc::new(equity_delta_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(equity_delta_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaCharge High".to_string(),
            calculator: std::sync::Arc::new(equity_delta_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ DeltaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(eq_delta_max),
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
