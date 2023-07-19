use crate::helpers::{get_optional_parameter, get_optional_parameter_array, ReturnMetric};
use crate::prelude::*;
use crate::sbm::common::{across_bucket_agg, rc_rcat_sens, total_vega_curv_sens, SBMChargeType};
use ndarray::{Array1, Array2, Axis};
use polars::prelude::IndexOrder;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, Float64Type, GetOutput},
    CPM,
};
use ultibi::{BaseMeasure, IntoLazy};

pub fn total_fx_vega_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Vega", "FX", total_vega_curv_sens()))
}

pub fn total_fx_vega_sens_weighted(op: &CPM) -> PolarsResult<Expr> {
    Ok(total_fx_vega_sens(op)? * col("SensWeights").list().get(lit(0)))
}

/// Sb Low == Sb Medium == Sb High
/// FX Vega Sb is identical to total_fx_vega_sens_weighted
pub(crate) fn fx_vega_sb(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

/// Interm Result: FX Vega Low Kb
pub(crate) fn fx_vega_kb_low(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

/// Interm Result: FX Vega Medium Kb
pub(crate) fn fx_vega_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

/// Interm Result: FX Vega High Kb
pub(crate) fn fx_vega_kb_high(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate FX Vega Low Capital charge
pub(crate) fn fx_vega_charge_low(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
///calculate FX Vega Medium Capital charge
pub(crate) fn fx_vega_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
///calculate FX Vega High Capital charge
pub(crate) fn fx_vega_charge_high(op: &CPM) -> PolarsResult<Expr> {
    fx_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn fx_vega_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();

    let fx_vega_rho = get_optional_parameter_array(
        op,
        format!("fx_opt_mat_vega_rho{_suffix}").as_str(),
        &scenario.fx_opt_mat_vega_rho,
    )?;
    let fx_vega_gamma = get_optional_parameter(
        op,
        format!("fx_vega_gamma{_suffix}").as_str(),
        &scenario.fx_delta_vega_gamma,
    )?;

    fx_vega_charge(fx_vega_rho, fx_vega_gamma, rtrn)
}

fn fx_vega_charge(
    fx_vega_rho: Array2<f64>,
    fx_vega_gamma: f64,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    Ok(apply_multiple(
        move |columns| {
            let df = df![
                "rcat" => &columns[0],
                "rc" =>   &columns[1],
                "b" =>    &columns[2],
                "y05" =>  &columns[3],
                "y1" =>   &columns[4],
                "y3" =>   &columns[5],
                "y5" =>   &columns[6],
                "y10" =>  &columns[7],
                "wght" => &columns[8]
            ]?;

            let df = df
                .lazy()
                .filter(col("rc").eq(lit("FX")).and(col("rcat").eq(lit("Vega"))))
                .groupby([col("b")])
                .agg([
                    (col("y05") * col("wght")).sum(),
                    (col("y1") * col("wght")).sum(),
                    (col("y3") * col("wght")).sum(),
                    (col("y5") * col("wght")).sum(),
                    (col("y10") * col("wght")).sum(),
                ])
                .select(&[col("*").exclude(["b"])])
                .fill_null(lit::<f64>(0.))
                .collect()?;

            let res_len = columns[0].len();

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let sens = df.to_ndarray::<Float64Type>(IndexOrder::Fortran)?;

            let sbs = sens.sum_axis(Axis(1));

            // Early return Kb or Sb, ie the required metric
            if let ReturnMetric::Sb = rtrn {
                return Ok(Some(Series::new("res", [sbs.sum()])));
            }

            // Interm step
            let _kbs = sens.dot(&fx_vega_rho);
            // Actual kbs
            // TODO use uninit here
            let mut kbs = Array1::<f64>::zeros(sbs.len());

            _kbs.axis_iter(Axis(0)).enumerate().for_each(|(i, arr)| {
                let a = unsafe { kbs.uget_mut(i) };
                *a = f64::max(arr.dot(&sens.row(i)), 0.).sqrt();
            });

            if let ReturnMetric::Kb = rtrn {
                return Ok(Some(Series::new("res", [kbs.sum()])));
            }

            let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), fx_vega_gamma);
            let zeros = Array1::zeros(kbs.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::DeltaVega)
        },
        &[
            col("RiskCategory"),
            col("RiskClass"),
            col("BucketBCBS"),
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            col("SensWeights").list().get(lit(0)),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    ))
}

/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn fx_vega_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        fx_vega_charge_low(op)?,
        fx_vega_charge_medium(op)?,
        fx_vega_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn fx_vega_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "FX VegaSens".to_string(),
            calculator: std::sync::Arc::new(total_fx_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(total_fx_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaSb".to_string(),
            calculator: std::sync::Arc::new(fx_vega_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaKb Low".to_string(),
            calculator: std::sync::Arc::new(fx_vega_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaKb Medium".to_string(),
            calculator: std::sync::Arc::new(fx_vega_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaKb High".to_string(),
            calculator: std::sync::Arc::new(fx_vega_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaCharge Low".to_string(),
            calculator: std::sync::Arc::new(fx_vega_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(fx_vega_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaCharge High".to_string(),
            calculator: std::sync::Arc::new(fx_vega_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "FX VegaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(fx_vega_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("FX"))),
            ),
            calc_params: vec![],
        }),
    ]
}
