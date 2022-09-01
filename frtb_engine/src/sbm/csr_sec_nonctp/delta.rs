//! CSR Sec non-CTP Delta Calculations
use crate::prelude::*;
use base_engine::prelude::*;
use ndarray::prelude::*;
use polars::prelude::*;

pub fn total_csr_sec_nonctp_delta_sens(_: &OCP) -> Expr {
    rc_rcat_sens("CSR_Sec_nonCTP", "Delta", total_delta_sens())
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
pub(crate) fn csr_sec_nonctp_delta_sens_weighted(_: &OCP) -> Expr {
    csr_sec_nonctp_delta_sens_weighted_05y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_1y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_3y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_5y_bcbs().fill_null(0.)
        + csr_sec_nonctp_delta_sens_weighted_10y_bcbs().fill_null(0.)
}
//Interm results
pub(crate) fn csr_sec_nonctp_delta_sb(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_low(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_sec_nonctp_delta_kb_high(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_low(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_high(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
/// calls csr_nonsec_delta_charge because the calculation is identical
fn csr_sec_nonctp_delta_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();

    let rho_tenor = get_optional_parameter(
        op,
        "base_csr_sec_nonctp_diff_tenor_rho",
        &scenario.base_csr_sec_nonctp_rho_tenor,
    );

    let rho_name = get_optional_parameter_vec(
        op,
        "base_csr_sec_nonctp_diff_name_rho",
        &scenario.csr_sec_nonctp_rho_diff_name_curv.to_vec(),
    );

    let rho_tranche = get_optional_parameter(
        op,
        "base_csr_sec_nonctp_diff_tranche_rho",
        &scenario.base_csr_sec_nonctp_rho_diff_tranche,
    );

    let gamma = get_optional_parameter_array(
        op,
        format!("base_csr_sec_nonctp_gamma{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_gamma,
    );

    // CTP calc is identical to nonSec, with the only exception on rho, gamma and number of buckets
    csr_sec_nonctp_delta_charge(
        rho_tenor,
        rho_name,
        rho_tranche,
        &scenario.scenario_fn,
        gamma,
        Some(25),
        "CSR_Sec_nonCTP",
        "Delta",
        rtrn,
    )
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
                "rcat" =>   columns[10].clone(),
                "rc" =>   columns[0].clone(),
                "rf" =>   columns[1].clone(),
                "tran"=>  columns[2].clone(),
                "b" =>    columns[3].clone(),
                "y05" =>  columns[4].clone(),
                "y1" =>   columns[5].clone(),
                "y3" =>   columns[6].clone(),
                "y5" =>   columns[7].clone(),
                "y10" =>  columns[8].clone(),
                "w"   =>  columns[9].clone()
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

            let ma = MeltArgs {
                id_vars: vec!["b".to_string(), "rf".to_string(), "tran".to_string()],
                value_vars: vec![
                    "y05".to_string(),
                    "y1".to_string(),
                    "y3".to_string(),
                    "y5".to_string(),
                    "y10".to_string(),
                ],
                variable_name: Some("tenor".to_string()),
                value_name: Some("weighted_sens".to_string()),
            };
            let df = df.melt2(ma).unwrap();
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
            )?;

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
            let res_len = columns[0].len();
            match rtrn {
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
            col("SensWeights").arr().get(0),
            col("RiskCategory"),
        ],
        GetOutput::from_type(DataType::Float64),
    )
}

/// Exporting Measures
pub(crate) fn csrsecnonctp_delta_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens".to_string(),
            calculator: Box::new(total_csr_sec_nonctp_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaSens_Weighted".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaKb_Low".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_kb_low),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaKb_Medium".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_kb_medium),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaKb_High".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_kb_high),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaSb".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_sb),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Low".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_low),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_Medium".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_medium),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
        Measure {
            name: "CSR_Sec_nonCTP_DeltaCharge_High".to_string(),
            calculator: Box::new(csr_sec_nonctp_delta_charge_high),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
        },
    ]
}