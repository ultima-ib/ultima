//! CSR Sec non-CTP Delta Calculations

use crate::prelude::*;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput, MeltArgs},
    BaseMeasure, IntoLazy, CPM,
};

use ndarray::Array2;

pub fn total_csr_sec_nonctp_delta_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_Sec_nonCTP", total_delta_sens()))
}
/// Helper functions

fn csr_sec_nonctp_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_Sec_nonCTP",
        "Sensitivity_05Y",
        "SensWeights",
        0,
    )
}
fn csr_sec_nonctp_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_Sec_nonCTP",
        "Sensitivity_1Y",
        "SensWeights",
        0,
    )
}
fn csr_sec_nonctp_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_Sec_nonCTP",
        "Sensitivity_3Y",
        "SensWeights",
        0,
    )
}
fn csr_sec_nonctp_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_Sec_nonCTP",
        "Sensitivity_5Y",
        "SensWeights",
        0,
    )
}
fn csr_sec_nonctp_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_Sec_nonCTP",
        "Sensitivity_10Y",
        "SensWeights",
        0,
    )
}

/// Total weighted CSR Sec nonCTP Delta
/// Not used in calculation
pub(crate) fn csr_sec_nonctp_delta_sens_weighted(_: &CPM) -> PolarsResult<Expr> {
    Ok(csr_sec_nonctp_delta_sens_weighted_05y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_1y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_3y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_5y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_10y_bcbs().fill_null(0.))
}
//Interm results
pub(crate) fn csr_sec_nonctp_delta_sb(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
/// calls csr_nonsec_delta_charge because the calculation is identical
fn csr_sec_nonctp_delta_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();

    let rho_tenor = get_optional_parameter(
        op,
        "csr_sec_nonctp_delta_diff_tenor_rho_base",
        &scenario.csr_sec_nonctp_delta_diff_tenor_rho_base,
    )?;

    let rho_name = get_optional_parameter_vec(
        op,
        "csr_sec_nonctp_delta_diff_name_rho_per_bucket_base",
        &scenario
            .csr_sec_nonctp_curv_diff_name_rho_per_bucket
            .to_vec(),
    )?;

    let rho_tranche = get_optional_parameter(
        op,
        "csr_sec_nonctp_delta_diff_tranche_rho_base",
        &scenario.csr_sec_nonctp_delta_diff_tranche_rho_base,
    )?;

    let gamma = get_optional_parameter_array(
        op,
        format!("csr_sec_nonctp_delta_gamma{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_delta_vega_gamma,
    )?;

    // CTP calc is identical to nonSec, with the only exception on rho, gamma and number of buckets
    Ok(csr_sec_nonctp_delta_charge(
        rho_tenor,
        rho_name,
        rho_tranche,
        scenario.scenario_fn,
        gamma,
        Some(25),
        "CSR_Sec_nonCTP",
        "Delta",
        rtrn,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn csr_sec_nonctp_delta_charge<F>(
    rho_tenor: f64,
    rho_name: Vec<f64>,
    rho_rft: f64,
    scenario_fn: F,
    gamma: Array2<f64>,
    special_bucket: Option<usize>,
    risk_class: &'static str,
    risk_cat: &'static str,
    rtrn: ReturnMetric,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    apply_multiple(
        move |columns| {
            let df = df![
                "rcat" =>   &columns[10],
                "rc" =>   &columns[0],
                "rf" =>   &columns[1],
                "tran"=>  &columns[2],
                "b" =>    &columns[3],
                "y05" =>  &columns[4],
                "y1" =>   &columns[5],
                "y3" =>   &columns[6],
                "y5" =>   &columns[7],
                "y10" =>  &columns[8],
                "w"   =>  &columns[9]
            ]?;

            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit(risk_class))
                        .and(col("rcat").eq(lit(risk_cat))),
                )
                .groupby([col("b"), col("rf"), col("tran")])
                .agg([
                    (col("y05") * col("w")).sum(),
                    (col("y1") * col("w")).sum(),
                    (col("y3") * col("w")).sum(),
                    (col("y5") * col("w")).sum(),
                    (col("y10") * col("w")).sum(),
                ])
                // No need to fill null here
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let ma = MeltArgs {
                id_vars: vec!["b".into(), "rf".into(), "tran".into()],
                value_vars: vec![
                    "y05".into(),
                    "y1".into(),
                    "y3".into(),
                    "y5".into(),
                    "y10".into(),
                ],
                streamable: false,
                variable_name: Some("tenor".into()),
                value_name: Some("weighted_sens".into()),
            };
            let df = df.melt2(ma)?;
            // 21.4.4 - 21.5.a
            let kbs_sbs = all_kbs_sbs_onsq(
                df,
                "tenor",
                rho_tenor,
                "rf",
                &rho_name,
                "tran",
                rho_rft,
                "weighted_sens",
                scenario_fn,
                special_bucket,
                &None,
            )?;

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
            let res_len = columns[0].len();
            match rtrn {
                ReturnMetric::Kb => return Ok(Some(Series::new("kbs", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("sbs", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }

            // 21.57 OR 325aj
            // Shape of gamma depends on regulation
            across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::DeltaVega)
        },
        &[
            col("RiskClass"),
            col("RiskFactor"),
            col("Tranche"),
            col("BucketBCBS"),
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            col("SensWeights").list().get(lit(0)),
            col("RiskCategory"),
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecnonctp_delta_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csr_sec_nonctp_delta_charge_low(op)?,
        csr_sec_nonctp_delta_charge_medium(op)?,
        csr_sec_nonctp_delta_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrsecnonctp_delta_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaSens".to_string(),
            calculator: std::sync::Arc::new(total_csr_sec_nonctp_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaKb Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaKb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaKb High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaSb".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaCharge Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaCharge High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_delta_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP DeltaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_delta_max),
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
