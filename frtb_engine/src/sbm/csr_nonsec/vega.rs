use crate::prelude::*;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput},
    BaseMeasure, IntoLazy, CPM,
};

use ndarray::Array2;

pub fn total_csrnonsec_vega_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Vega", "CSR_nonSec", total_vega_curv_sens()))
}

pub fn total_csrnonsec_vega_sens_weighted_bcbs(op: &CPM) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;

    match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => total_csrnonsec_vega_sens(op)
            .map(|expr| expr * col("SensWeightsCRR2").list().get(lit(0))),
        Jurisdiction::BCBS => {
            total_csrnonsec_vega_sens(op).map(|expr| expr * col("SensWeights").list().get(lit(0)))
        }
    }
}

///calculate CSR Non Sec Interm Result
pub(crate) fn csr_nonsec_vega_sb(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_vega_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csr_nonsec_vega_charge_distributor(
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
            Vec::from(scenario.csr_nonsec_delta_diff_name_rho_per_bucket_base_crr2),
            &scenario.base_vega_rho,
            &scenario.csr_nonsec_delta_vega_gamma_crr2,
            Some("18"),
        ),

        Jurisdiction::BCBS => (
            col("SensWeights").list().get(lit(0)),
            col("BucketBCBS"),
            Vec::from(scenario.csr_nonsec_delta_vega_diff_name_rho_per_bucket_base_bcbs),
            &scenario.base_vega_rho,
            &scenario.csr_nonsec_delta_vega_gamma_bcbs,
            Some("16"),
        ),
    };

    let csr_gamma = get_optional_parameter_array(
        op,
        format!("csr_nonsec_vega_gamma{_suffix}").as_str(),
        gamma,
    )?;
    let base_csr_rho_bucket = get_optional_parameter_vec(
        op,
        "csr_nonsec_vega_diff_name_rho_per_bucket_base",
        &name_rho_vec,
    )?;
    let csr_vega_rho =
        get_optional_parameter_array(op, "csr_nonsec_opt_mat_vega_rho_base", rho_opt)?;

    Ok(csr_nonsec_vega_charge(
        weight,
        bucket_col,
        scenario.scenario_fn,
        csr_vega_rho,
        base_csr_rho_bucket,
        csr_gamma,
        special_bucket,
        "CSR_nonSec",
        "Vega",
        rtrn,
    ))
}

/// Used by CSR nonSec, CSR Sec CTP Vegas
#[allow(clippy::too_many_arguments)]
pub(crate) fn csr_nonsec_vega_charge<F>(
    weight: Expr,
    bucket_col: Expr,
    scenario_fn: F,
    opt_mat_rho: Array2<f64>,
    rho_diff_curve: Vec<f64>,
    gamma: Array2<f64>,
    special_bucket: Option<&'static str>,
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
                "rc" =>   &columns[0],
                "rf" =>   &columns[1],
                "b" =>    &columns[2],
                "y05" =>  &columns[3],
                "y1" =>   &columns[4],
                "y3" =>   &columns[5],
                "y5" =>   &columns[6],
                "y10" =>  &columns[7],
                "w" =>    &columns[8],
                "rcat" => &columns[9],
            ]?;

            // concat_lst is actually slower than
            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit(risk_class))
                        .and(col("rcat").eq(lit(risk_cat))),
                )
                .groupby([col("b"), col("rf")])
                .agg([
                    (col("y05") * col("w")).sum(),
                    (col("y1") * col("w")).sum(),
                    (col("y3") * col("w")).sum(),
                    (col("y5") * col("w")).sum(),
                    (col("y10") * col("w")).sum(),
                ])
                //.fill_null(lit::<f64>(0.))
                .collect()?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            let kbs_sbs = all_kbs_sbs_single_type(
                df,
                &opt_mat_rho,
                &rho_diff_curve,
                scenario_fn,
                &["y05", "y1", "y3", "y5", "y10"],
                special_bucket,
            )?;

            let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

            match rtrn {
                ReturnMetric::Kb => return Ok(Some(Series::new("kbs", [kbs.iter().sum::<f64>()]))),
                ReturnMetric::Sb => return Ok(Some(Series::new("sbs", [sbs.iter().sum::<f64>()]))),
                _ => (),
            }

            across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
        },
        &[
            col("RiskClass"),
            col("RiskFactor"),
            bucket_col,
            //y05, y1, y3, y5, y10,
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            weight, // risk weight
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
fn csrnonsec_vega_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csr_nonsec_vega_charge_low(op)?,
        csr_nonsec_vega_charge_medium(op)?,
        csr_nonsec_vega_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrnonsec_vega_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaSens".to_string(),
            calculator: std::sync::Arc::new(total_csrnonsec_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(total_csrnonsec_vega_sens_weighted_bcbs),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaSb".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaCharge Low".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaKb Low".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaKb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaCharge High".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaKb High".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_vega_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec VegaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_vega_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
    ]
}
