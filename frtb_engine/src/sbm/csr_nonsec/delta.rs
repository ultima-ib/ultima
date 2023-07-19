//! CSR non-Sec Delta Calculations
use crate::helpers::*;
use crate::sbm::common::*;
use ndarray::Array2;
use ultibi::{
    polars::prelude::{apply_multiple, df, max_horizontal, DataType, GetOutput},
    BaseMeasure, IntoLazy, CPM,
};

use crate::prelude::*;

pub fn total_csr_nonsec_delta_sens(_: &CPM) -> PolarsResult<Expr> {
    Ok(rc_rcat_sens("Delta", "CSR_nonSec", total_vega_curv_sens()))
}
/// Helper functions

fn csr_nonsec_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta", "CSR_nonSec", "Sensitivity_05Y", "SensWeights", 0)
}
fn csr_nonsec_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta", "CSR_nonSec", "Sensitivity_1Y", "SensWeights", 1)
}
fn csr_nonsec_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta", "CSR_nonSec", "Sensitivity_3Y", "SensWeights", 2)
}
fn csr_nonsec_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta", "CSR_nonSec", "Sensitivity_5Y", "SensWeights", 3)
}
fn csr_nonsec_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta", "CSR_nonSec", "Sensitivity_10Y", "SensWeights", 4)
}

//CRR2
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_05y_crr2() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_nonSec",
        "Sensitivity_05Y",
        "SensWeightsCRR2",
        0,
    )
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_1y_crr2() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_nonSec",
        "Sensitivity_1Y",
        "SensWeightsCRR2",
        1,
    )
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_3y_crr2() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_nonSec",
        "Sensitivity_3Y",
        "SensWeightsCRR2",
        2,
    )
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_5y_crr2() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_nonSec",
        "Sensitivity_5Y",
        "SensWeightsCRR2",
        3,
    )
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_10y_crr2() -> Expr {
    rc_tenor_weighted_sens(
        "Delta",
        "CSR_nonSec",
        "Sensitivity_10Y",
        "SensWeightsCRR2",
        4,
    )
}

/// Total CSR non-Sec Delta
/// Not used in calculation
pub(crate) fn csr_nonsec_delta_sens_weighted(op: &CPM) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;

    match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => Ok(csr_nonsec_delta_sens_weighted_05y_crr2().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_1y_crr2().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_3y_crr2().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_5y_crr2().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_10y_crr2().fill_null(0.)),
        Jurisdiction::BCBS => Ok(csr_nonsec_delta_sens_weighted_05y_bcbs().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_1y_bcbs().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_3y_bcbs().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_5y_bcbs().fill_null(0.)
            + csr_nonsec_delta_sens_weighted_10y_bcbs().fill_null(0.)),
    }
}

//Interm Results
///Sb is same for each scenario
pub(crate) fn csr_nonsec_delta_sb(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

pub(crate) fn csr_nonsec_delta_kb_low(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_nonsec_delta_kb_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_nonsec_delta_kb_high(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_nonsec_delta_charge_low(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_nonsec_delta_charge_medium(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_nonsec_delta_charge_high(op: &CPM) -> PolarsResult<Expr> {
    csr_nonsec_delta_charge_distributor(op, &HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
fn csr_nonsec_delta_charge_distributor(
    op: &CPM,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> PolarsResult<Expr> {
    let juri: Jurisdiction = get_jurisdiction(op)?;
    let _suffix = scenario.as_str();
    let (weight, bucket_col, name_rho_vec, gamma, n_buckets, special_bucket) = match juri {
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (
            [
                col("SensWeightsCRR2").list().get(lit(0)),
                col("SensWeightsCRR2").list().get(lit(1)),
                col("SensWeightsCRR2").list().get(lit(2)),
                col("SensWeightsCRR2").list().get(lit(3)),
                col("SensWeightsCRR2").list().get(lit(4)),
            ],
            col("BucketCRR2"),
            Vec::from(scenario.csr_nonsec_delta_diff_name_rho_per_bucket_base_crr2),
            &scenario.csr_nonsec_delta_vega_gamma_crr2,
            20usize,
            Some(18usize),
        ),

        Jurisdiction::BCBS => (
            [
                col("SensWeights").list().get(lit(0)),
                col("SensWeights").list().get(lit(1)),
                col("SensWeights").list().get(lit(2)),
                col("SensWeights").list().get(lit(3)),
                col("SensWeights").list().get(lit(4)),
            ],
            col("BucketBCBS"),
            Vec::from(scenario.csr_nonsec_delta_vega_diff_name_rho_per_bucket_base_bcbs),
            &scenario.csr_nonsec_delta_vega_gamma_bcbs,
            18,
            Some(16),
        ),
    };

    let base_csr_nonsec_rho_tenor = get_optional_parameter(
        op,
        "csr_nonsec_delta_diff_tenor_rho_base",
        &scenario.csr_nonsec_delta_diff_tenor_rho_base,
    )?;

    let name_rho_vec = get_optional_parameter_vec(
        op,
        "csr_nonsec_delta_diff_name_rho_per_bucket_base",
        &name_rho_vec,
    )?;

    let base_csr_nonsec_rho_basis = get_optional_parameter(
        op,
        "csr_nonsec_delta_diff_basis_rho_base",
        &scenario.csr_nonsec_delta_diff_basis_rho_base,
    )?;

    let gamma = get_optional_parameter_array(
        op,
        format!("csr_nonsec_delta_gamma{_suffix}").as_str(),
        gamma,
    )?;

    Ok(csr_nonsec_delta_charge(
        weight,
        base_csr_nonsec_rho_tenor,
        name_rho_vec,
        base_csr_nonsec_rho_basis,
        bucket_col,
        scenario.scenario_fn,
        gamma,
        n_buckets,
        special_bucket,
        "CSR_nonSec",
        "Delta",
        rtrn,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn csr_nonsec_delta_charge<F>(
    weight: [Expr; 5],
    base_tenor_rho: f64,
    rho_name: Vec<f64>,
    rho_basis: f64,
    bucket_col: Expr,
    scenario_fn: F,
    gamma: Array2<f64>,
    n_buckets: usize,
    special_bucket: Option<usize>,
    risk_class: &'static str,
    risk_cat: &'static str,
    rtrn: ReturnMetric,
) -> Expr
where
    F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,
{
    let [w1, w2, w3, w4, w5] = weight;
    apply_multiple(
        move |columns| {
            //let now = Instant::now();
            let df = df![
                "rc" =>   &columns[0],
                "rf" =>   &columns[1],
                "rft" =>  &columns[2],
                "b" =>    &columns[3],
                "y05" =>  &columns[4],
                "y1" =>   &columns[5],
                "y3" =>   &columns[6],
                "y5" =>   &columns[7],
                "y10" =>  &columns[8],
                "rcat" => &columns[9],
                "w1" =>   &columns[10],
                "w2" =>   &columns[11],
                "w3" =>   &columns[12],
                "w4" =>   &columns[13],
                "w5" =>   &columns[14]
            ]?;

            if df.height() == 0 {
                return Ok(Some(Series::new("res", [0.])));
            };

            // concat_lst is actually slower than
            let df = df
                .lazy()
                .filter(
                    col("rc")
                        .eq(lit(risk_class))
                        .and(col("rcat").eq(lit(risk_cat))),
                )
                .with_columns([
                    col("y05") * col("w1"),
                    col("y1") * col("w2"),
                    col("y3") * col("w3"),
                    col("y5") * col("w4"),
                    col("y10") * col("w5"),
                ])
                .with_columns([
                    when(col("rft").eq(lit("Bond")))
                        .then(col("y05").alias("Bond_y05"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("CDS")))
                        .then(col("y05").alias("CDS_y05"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("Bond")))
                        .then(col("y1").alias("Bond_y1"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("CDS")))
                        .then(col("y1").alias("CDS_y1"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("Bond")))
                        .then(col("y3").alias("Bond_y3"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("CDS")))
                        .then(col("y3").alias("CDS_y3"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("Bond")))
                        .then(col("y5").alias("Bond_y5"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("CDS")))
                        .then(col("y5").alias("CDS_y5"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("Bond")))
                        .then(col("y10").alias("Bond_y10"))
                        .otherwise(NULL.lit()),
                    when(col("rft").eq(lit("CDS")))
                        .then(col("y10").alias("CDS_y10"))
                        .otherwise(NULL.lit()),
                ])
                .groupby([col("b"), col("rf")])
                .agg([
                    col("Bond_y05").sum(),
                    col("CDS_y05").sum(),
                    col("Bond_y1").sum(),
                    col("CDS_y1").sum(),
                    col("Bond_y3").sum(),
                    col("CDS_y3").sum(),
                    col("Bond_y5").sum(),
                    col("CDS_y5").sum(),
                    col("Bond_y10").sum(),
                    col("CDS_y10").sum(),
                ])
                .collect()?;

            let kbs_sbs = all_kbs_sbs_two_types(
                df,
                n_buckets,
                &rho_name,
                rho_basis,
                scenario_fn,
                special_bucket,
                &[
                    ("Bond_y05", "CDS_y05"),
                    ("Bond_y1", "CDS_y1"),
                    ("Bond_y3", "CDS_y3"),
                    ("Bond_y5", "CDS_y5"),
                    ("Bond_y10", "CDS_y10"),
                ],
                Some(base_tenor_rho),
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
            col("RiskFactorType"),
            bucket_col,
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            col("RiskCategory"),
            w1,
            w2,
            w3,
            w4,
            w5,
        ],
        GetOutput::from_type(DataType::Float64),
        true,
    )
}

/// Returns max of three scenarios
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn csrnonsec_delta_max(op: &CPM) -> PolarsResult<Expr> {
    Ok(max_horizontal(&[
        csr_nonsec_delta_charge_low(op)?,
        csr_nonsec_delta_charge_medium(op)?,
        csr_nonsec_delta_charge_high(op)?,
    ]))
}

/// Exporting Measures
pub(crate) fn csrnonsec_delta_measures() -> Vec<Measure> {
    vec![
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaSens".to_string(),
            calculator: std::sync::Arc::new(total_csr_nonsec_delta_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaSens Weighted".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaSb".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_sb),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaKb Low".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_kb_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaKb Medium".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_kb_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaKb High".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_kb_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaCharge Low".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_charge_low),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaCharge Medium".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_charge_medium),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaCharge High".to_string(),
            calculator: std::sync::Arc::new(csr_nonsec_delta_charge_high),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
        Measure::Base(BaseMeasure {
            name: "CSR nonSec DeltaCharge MAX".to_string(),
            calculator: std::sync::Arc::new(csrnonsec_delta_max),
            aggregation: Some("scalar".into()),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Delta"))
                    .and(col("RiskClass").eq(lit("CSR_nonSec"))),
            ),
            calc_params: vec![],
        }),
    ]
}
