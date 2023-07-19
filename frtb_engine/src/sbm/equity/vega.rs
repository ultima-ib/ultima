use crate::prelude::*;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput},
    BaseMeasure, IntoLazy, CPM,
};

use ndarray::Array2;

pub fn total_eq_vega_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Vega", "Equity", total_vega_curv_sens()))
}

pub fn total_eq_vega_sens_weighted(op: &CPM) -> PolarsResult<Expr> {
    total_eq_vega_sens(op).map(|expr| expr * col("SensWeights").list().get(lit(0)))
}
///Interm Result
pub(crate) fn equity_vega_sb(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn equity_vega_kb_low(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_low(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn equity_vega_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn equity_vega_kb_high(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_high(op: &CPM) -> PolarsResult<Expr> {
    equity_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn equity_vega_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();
    //TODO check
    let eq_gamma = get_optional_parameter_array(
        op,
        format!("eq_vega_gamma{_suffix}").as_str(),
        &scenario.eq_delta_vega_gamma,
    )?;
    let base_eq_rho_bucket = get_optional_parameter(
        op,
        "eq_vega_rho_diff_name_per_bucket_base",
        &scenario.eq_delta_vega_diff_name_rho_per_bucket_base,
    )?;
    let eq_vega_rho =
        get_optional_parameter_array(op, "eq_opt_mat_vega_rho_base", &scenario.base_vega_rho)?;

    Ok(equity_vega_charge(
        eq_vega_rho,
        eq_gamma,
        base_eq_rho_bucket.to_vec(),
        scenario.scenario_fn,
        rtrn,
        Some("11"),
        "Equity",
    ))
}

/// calculate Equity Vega Capital charge. Used for Commodity also
pub(crate) fn equity_vega_charge<F>(
    opt_mat_rho: Array2<f64>,
    gamma: Array2<f64>,
    eq_rho_bucket: Vec<f64>,
    scenario_fn: F,
    rtrn: ReturnMetric,
    special_bucket: Option<&'static str>,
    rc: &'static str,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    // inner function
    apply_multiple(
        move |columns| {
            let df = df![
                "rcat" => &columns[0],
                "rc" =>   &columns[1],
                "b" =>    &columns[2],
                "rf" =>   &columns[3],
                "y05" =>  &columns[4],
                "y1" =>   &columns[5],
                "y3" =>   &columns[6],
                "y5" =>   &columns[7],
                "y10" =>  &columns[8],
                "wght" => &columns[9],
            ]?;

            // 21.4.3 - Netting
            let df = df
                .lazy()
                .filter(col("rc").eq(lit(rc)).and(col("rcat").eq(lit("Vega"))))
                .groupby([col("b"), col("rf")])
                .agg([
                    (col("y05") * col("wght")).sum().alias("y05"),
                    (col("y1") * col("wght")).sum().alias("y1"),
                    (col("y3") * col("wght")).sum().alias("y3"),
                    (col("y5") * col("wght")).sum().alias("y5"),
                    (col("y10") * col("wght")).sum().alias("y10"),
                ])
                //.fill_null(0.)
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };
            // Compute present buckets

            // USE all_kbs_sbs here, this helps skipping unnecessary
            // iterations over buckets which are not present
            let kbs_sbs = all_kbs_sbs_single_type(
                df,
                &opt_mat_rho,
                &eq_rho_bucket,
                scenario_fn,
                &["y05", "y1", "y3", "y5", "y10"],
                special_bucket,
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
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
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
fn eq_vega_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        equity_vega_charge_low(op)?,
        equity_vega_charge_medium(op)?,
        equity_vega_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn eq_vega_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "EQ VegaSens".to_string(),
            calculator: std::sync::Arc::new(total_eq_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(total_eq_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaSb".to_string(),
            calculator: std::sync::Arc::new(equity_vega_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaKb Low".to_string(),
            calculator: std::sync::Arc::new(equity_vega_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaCharge Low".to_string(),
            calculator: std::sync::Arc::new(equity_vega_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaKb Medium".to_string(),
            calculator: std::sync::Arc::new(equity_vega_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(equity_vega_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaKb High".to_string(),
            calculator: std::sync::Arc::new(equity_vega_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaCharge High".to_string(),
            calculator: std::sync::Arc::new(equity_vega_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "EQ VegaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(eq_vega_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Equity"))),
            ),
            calc_params: vec![],
        }),
    ]
}
