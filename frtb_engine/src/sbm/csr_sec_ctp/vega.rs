use crate::{prelude::*, sbm::csr_nonsec::vega::csr_nonsec_vega_charge};
use ultibi::{polars::prelude::max_horizontal, BaseMeasure, CPM};

pub fn total_csrsecctp_vega_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Vega", "CSR_Sec_CTP", total_vega_curv_sens()))
}

pub fn total_csrsecctp_vega_sens_weighted(op: &CPM) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;

    match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => total_csrsecctp_vega_sens(op)
            .map(|expr| expr * col("SensWeightsCRR2").list().get(lit(0))),
        Jurisdiction::BCBS => {
            total_csrsecctp_vega_sens(op).map(|expr| expr * col("SensWeights").list().get(lit(0)))
        }
    }
}

///calculate CSR Sec CTP Interm Result
pub(crate) fn csrsecctp_vega_sb(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csrsecctp_vega_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Sec CTP Vega Low Capital charge
pub(crate) fn csrsecctp_vega_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csrsecctp_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csrsecctp_vega_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;
    let _suffix = scenario.as_str();

    let (weight, bucket_col, name_rho_vec, rho_opt, gamma, special_bucket) = match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (
            col("SensWeightsCRR2").list().get(lit(0)),
            col("BucketCRR2"),
            Vec::from(scenario.csr_ctp_delta_vega_diff_name_rho_per_bucket_base_crr2),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_delta_vega_gamma_crr2,
            None,
        ),

        Jurisdiction::BCBS => (
            col("SensWeights").list().get(lit(0)),
            col("BucketBCBS"),
            Vec::from(scenario.csr_ctp_delta_vega_diff_name_rho_per_bucket_base_bcbs),
            &scenario.base_vega_rho,
            &scenario.csr_ctp_delta_vega_gamma_bcbs,
            None,
        ),
    };

    let csr_gamma =
        get_optional_parameter_array(op, format!("csr_ctp_vega_gamma{_suffix}").as_str(), gamma)?;
    let base_csr_rho_bucket = get_optional_parameter_vec(
        op,
        "csr_ctp_vega_diff_name_rho_per_bucket_base",
        &name_rho_vec,
    )?;
    let csr_vega_rho = get_optional_parameter_array(op, "csr_ctp_opt_mat_vega_rho_base", rho_opt)?;

    Ok(csr_nonsec_vega_charge(
        weight,
        bucket_col,
        scenario.scenario_fn,
        csr_vega_rho,
        base_csr_rho_bucket,
        csr_gamma,
        special_bucket,
        "CSR_Sec_CTP",
        "Vega",
        rtrn,
    ))
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrsecctp_vega_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csrsecctp_vega_charge_low(op)?,
        csrsecctp_vega_charge_medium(op)?,
        csrsecctp_vega_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrsecctp_vega_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaSens".to_string(),
            calculator: std::sync::Arc::new(total_csrsecctp_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(total_csrsecctp_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaSb".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaCharge Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaKb Low".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaKb Medium".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaCharge High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaKb High".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR Sec CTP VegaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrsecctp_vega_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_Sec_CTP"))),
            ),
            calc_params: vec![],
        }),
    ]
}
