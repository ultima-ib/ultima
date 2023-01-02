use crate::{prelude::*, sbm::equity::vega::equity_vega_charge};
use base_engine::polars::prelude::max_exprs;

pub fn total_com_vega_sens(_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "Commodity", total_vega_curv_sens())
}

pub fn total_com_vega_sens_weighted(op: &OCP) -> Expr {
    total_com_vega_sens(op) * col("SensWeights").arr().get(lit(0))
}
///Interm Result
pub(crate) fn com_vega_sb(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn com_vega_kb_low(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_low(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn com_vega_kb_medium(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_medium(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn com_vega_kb_high(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Equity Vega Low Capital charge
pub(crate) fn com_vega_charge_high(op: &OCP) -> Expr {
    com_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// TODO test
fn com_vega_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let _suffix = scenario.as_str();

    let com_gamma = get_optional_parameter_array(
        op,
        format!("com_vega_gamma{_suffix}").as_str(),
        &scenario.com_delta_vega_gamma,
    );
    let com_rho_bucket = get_optional_parameter(
        op,
        "com_vega_rho_bucket_base",
        &scenario.com_delta_vega_diff_cty_rho_per_bucket_base,
    );
    let com_vega_rho =
        get_optional_parameter_array(op, "com_opt_mat_vega_rho_base", &scenario.base_vega_rho);

    // The approach is identical to Equity
    equity_vega_charge(
        com_vega_rho,
        com_gamma,
        com_rho_bucket.to_vec(),
        scenario.scenario_fn,
        rtrn,
        None,
        "Commodity",
    )
}
/// Returns max of three scenarios
///
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn com_vega_max(op: &OCP) -> Expr {
    max_exprs(&[
        com_vega_charge_low(op),
        com_vega_charge_medium(op),
        com_vega_charge_high(op),
    ])
}

/// Exporting Measures
pub(crate) fn com_vega_measures() -> Vec<Measure> {
    vec![
        Measure {
            name: "Commodity VegaSens".to_string(),
            calculator: Box::new(total_com_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaSens Weighted".to_string(),
            calculator: Box::new(total_com_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaSb".to_string(),
            calculator: Box::new(com_vega_sb),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaKb Low".to_string(),
            calculator: Box::new(com_vega_kb_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaCharge Low".to_string(),
            calculator: Box::new(com_vega_charge_low),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaKb Medium".to_string(),
            calculator: Box::new(com_vega_kb_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaCharge Medium".to_string(),
            calculator: Box::new(com_vega_charge_medium),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaKb High".to_string(),
            calculator: Box::new(com_vega_kb_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaCharge High".to_string(),
            calculator: Box::new(com_vega_charge_high),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
        Measure {
            name: "Commodity VegaCharge MAX".to_string(),
            calculator: Box::new(com_vega_max),
            aggregation: Some("scalar"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("Commodity"))),
            ),
        },
    ]
}
