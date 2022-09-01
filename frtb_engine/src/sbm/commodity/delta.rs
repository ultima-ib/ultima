//! Commodity Delta Risk Charge
//! TODO Commodity RiskFactor should be of the form ...CCY (same as FX, where CCY is the reporting CCY)

use crate::prelude::*;
use base_engine::prelude::*;

use ndarray::prelude::*;
use polars::prelude::*;

pub fn total_commodity_delta_sens(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Commodity", total_delta_sens())
}

/// Total Commodity Delta
pub(crate) fn commodity_delta_sens_weighted(op: &OCP) -> Expr {
    total_commodity_delta_sens(op) * col("SensWeights").arr().get(0)
}

/// Interm Result: Commodity Delta Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn commodity_delta_sb(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)
}
/// Interm Result: Commodity Kb Low
pub(crate) fn commodity_delta_kb_low(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}
/// Interm Result: Commodity Kb Medium
pub(crate) fn commodity_delta_kb_medium(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}
/// Interm Result: Commodity Kb High
pub(crate) fn commodity_delta_kb_high(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate commodity Delta Low Capital charge
pub(crate) fn commodity_delta_charge_low(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate commodity Delta Medium Capital charge
pub(crate) fn commodity_delta_charge_medium(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate commodity Delta High Capital charge
pub(crate) fn commodity_delta_charge_high(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn commodity_delta_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();

    let com_gamma = get_optional_parameter_array(
        op,
        format!("commodity_delta_gamma{_suffix}").as_str(),
        &scenario.com_gamma,
    );
    let commodity_rho_bucket = get_optional_parameter(
        op,
        format!("commodity_delta_rho_bucket{_suffix}").as_str(),
        &scenario.base_com_delta_rho_cty,
    );
    let commodity_rho_diff_loc = get_optional_parameter(
        op,
        format!("commodity_delta_rho_diff_{_suffix}").as_str(),
        &scenario.base_com_rho_basis_diff,
    );
    let commodity_rho_diff_tenor = get_optional_parameter(
        op,
        format!("commodity_delta_rho_diff_{_suffix}").as_str(),
        &scenario.base_com_rho_tenor,
    );

    commodity_delta_charge(
        commodity_rho_bucket,
        com_gamma,
        commodity_rho_diff_loc,
        commodity_rho_diff_tenor,
        scenario.scenario_fn,
        rtrn,
    )
}

fn commodity_delta_charge<F>(
    bucket_rho_cty: [f64; 11],
    com_gamma: Array2<f64>,
    com_rho_base_diff_loc: f64,
    rho_tenor: f64,
    scenario_fn: F,
    rtrn: ReturnMetric,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    apply_multiple(
        move |columns| {
            let df = df![
                "rcat"=>  columns[16].clone(),
                "rc" =>   columns[0].clone(),
                "rf" =>   columns[1].clone(),
                "loc" =>  columns[2].clone(),
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
                "w"   =>  columns[15].clone(),
            ]?;

            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit("Commodity"))
                        .and(col("rcat").eq(lit("Delta"))),
                )
                .groupby([col("b"), col("rf"), col("loc")])
                .agg([
                    (col("y0") * col("w")).sum(),
                    (col("y025") * col("w")).sum(),
                    (col("y05") * col("w")).sum(),
                    (col("y1") * col("w")).sum(),
                    (col("y2") * col("w")).sum(),
                    (col("y3") * col("w")).sum(),
                    (col("y5") * col("w")).sum(),
                    (col("y10") * col("w")).sum(),
                    (col("y15") * col("w")).sum(),
                    (col("y20") * col("w")).sum(),
                    (col("y30") * col("w")).sum(),
                ])
                // No need to fill null here
                .collect()?;

            let ma = MeltArgs {
                id_vars: vec!["b".to_string(), "rf".to_string(), "loc".to_string()],
                value_vars: vec![
                    "y0".to_string(),
                    "y025".to_string(),
                    "y05".to_string(),
                    "y1".to_string(),
                    "y2".to_string(),
                    "y3".to_string(),
                    "y5".to_string(),
                    "y10".to_string(),
                    "y15".to_string(),
                    "y20".to_string(),
                    "y30".to_string(),
                ],
                variable_name: Some("tenor".to_string()),
                value_name: Some("weighted_sens".to_string()),
            };
            let df = df.melt2(ma)?;

            let kbs_sbs = all_kbs_sbs_onsq(
                df,
                "tenor",
                rho_tenor,
                "rf",
                &bucket_rho_cty,
                "loc",
                com_rho_base_diff_loc,
                "weighted_sens",
                scenario_fn,
                None,
            )?;

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
            let res_len = columns[0].len();

            match rtrn {
                ReturnMetric::Kb => {
                    return Ok(
                        Float64Chunked::from_vec("Res", vec![kbs.iter().sum(); res_len])
                            .into_series(),
                    )
                }
                ReturnMetric::Sb => {
                    return Ok(
                        Float64Chunked::from_vec("Res", vec![sbs.iter().sum(); res_len])
                            .into_series(),
                    )
                }
                _ => (),
            }
            across_bucket_agg(kbs, sbs, &com_gamma, res_len, SBMChargeType::DeltaVega)
        },
        &[
            col("RiskClass"),
            col("RiskFactor"),
            col("CommodityLocation"),
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
            col("SensWeights").arr().get(0),
            col("RiskCategory"),
        ],
        GetOutput::from_type(DataType::Float64),
    )
}

/// Returns max of three scenarios
/// 
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn com_delta_max(op: &OCP) -> Expr {
    max_exprs(&[commodity_delta_charge_low(op), commodity_delta_charge_medium(op), commodity_delta_charge_high(op)])
}

/// Exporting Measures
pub(crate) fn com_delta_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "Commodity_DeltaSens".to_string(),
            calculator: Box::new(total_commodity_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaSens_Weighted".to_string(),
            calculator: Box::new(commodity_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaSb".to_string(),
            calculator: Box::new(commodity_delta_sb),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaKb_Low".to_string(),
            calculator: Box::new(commodity_delta_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaKb_Medium".to_string(),
            calculator: Box::new(commodity_delta_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaKb_High".to_string(),
            calculator: Box::new(commodity_delta_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaCharge_Low".to_string(),
            calculator: Box::new(commodity_delta_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaCharge_Medium".to_string(),
            calculator: Box::new(commodity_delta_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaCharge_High".to_string(),
            calculator: Box::new(commodity_delta_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity_DeltaCharge_MAX".to_string(),
            calculator: Box::new(com_delta_max),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
    ]
}