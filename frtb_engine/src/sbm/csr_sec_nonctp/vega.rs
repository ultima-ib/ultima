use crate::{prelude::*, sbm::equity::vega::equity_vega_charge};
use ultibi::{polars::prelude::max_horizontal, BaseMeasure, CPM};

pub fn total_csr_sec_nonctp_vega_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens(
        "Vega",
        "CSR_Sec_nonCTP",
        total_vega_curv_sens(),
    ))
}

pub fn total_csr_sec_nonctp_vega_sens_weighted(op: &CPM) -> PolarsResult<Expr> {
    total_csr_sec_nonctp_vega_sens(op).map(|expr| expr * col("SensWeights").list().get(lit(0)))
}
///Interm Result
pub(crate) fn csr_sec_nonctp_vega_sb(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}
pub(crate) fn csr_sec_nonctp_vega_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_sec_nonctp_vega_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_sec_nonctp_vega_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate Sec nonCTP Vega Low Capital charge
pub(crate) fn csr_sec_nonctp_vega_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csr_sec_nonctp_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csr_sec_nonctp_vega_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let _suffix = scenario.as_str();
    //TODO check
    let csr_sec_nonctp_gamma = get_optional_parameter_array(
        op,
        format!("csr_sec_nonctp_vega_gamma{_suffix}").as_str(),
        &scenario.csr_sec_nonctp_delta_vega_gamma,
    )?;
    let csr_sec_nonctp_rho_bucket = get_optional_parameter(
        op,
        "csr_sec_nonctp_vega_rho_diff_name_per_bucket_base",
        &scenario.csr_sec_nonctp_delta_vega_diff_name_rho_per_bucket_base,
    )?;
    let csr_sec_nonctp_vega_rho = get_optional_parameter_array(
        op,
        "csr_sec_nonctp_opt_mat_vega_rho_base",
        &scenario.base_vega_rho,
    )?;

    Ok(equity_vega_charge(
        csr_sec_nonctp_vega_rho,
        csr_sec_nonctp_gamma,
        csr_sec_nonctp_rho_bucket.to_vec(),
        scenario.scenario_fn,
        rtrn,
        Some("25"),
        "CSR_Sec_nonCTP",
    ))
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecnonctp_vega_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csr_sec_nonctp_vega_charge_low(op)?,
        csr_sec_nonctp_vega_charge_medium(op)?,
        csr_sec_nonctp_vega_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrsecnonctp_vega_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaSens".to_string(),
            calculator: std::sync::Arc::new(total_csr_sec_nonctp_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(total_csr_sec_nonctp_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaSb".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaCharge Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaKb Low".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaKb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaCharge High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaKb High".to_string(),
            calculator: std::sync::Arc::new(csr_sec_nonctp_vega_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec nonCTP VegaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrsecnonctp_vega_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_nonCTP"))),
            ),
            calc_params: vec![],
        }),
    ]
}
