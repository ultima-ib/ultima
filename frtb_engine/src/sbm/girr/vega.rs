use crate::helpers::{get_optional_parameter, get_optional_parameter_array, ReturnMetric};
use crate::prelude::*;
use crate::sbm::common::{
    across_bucket_agg, option_maturity_rho, rc_rcat_sens, rc_tenor_weighted_sens,
    total_vega_curv_sens, SBMChargeType,
};
use base_engine::prelude::*;

#[cfg(feature = "CRR2")]
use super::delta::build_girr_crr2_gamma;
use ndarray::{s, Array1, Array2};
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub fn total_ir_vega_sens(_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "GIRR", total_vega_curv_sens())
}

fn girr_vega_sens_weighted_05y() -> Expr {
    rc_tenor_weighted_sens("Vega", "GIRR", "Sensitivity_05Y", "SensWeights", 0)
}
fn girr_vega_sens_weighted_1y() -> Expr {
    rc_tenor_weighted_sens("Vega", "GIRR", "Sensitivity_1Y", "SensWeights", 0)
}
fn girr_vega_sens_weighted_3y() -> Expr {
    rc_tenor_weighted_sens("Vega", "GIRR", "Sensitivity_3Y", "SensWeights", 0)
}
fn girr_vega_sens_weighted_5y() -> Expr {
    rc_tenor_weighted_sens("Vega", "GIRR", "Sensitivity_5Y", "SensWeights", 0)
}
fn girr_vega_sens_weighted_10y() -> Expr {
    rc_tenor_weighted_sens("Vega", "GIRR", "Sensitivity_10Y", "SensWeights", 0)
}

/// Total GIRR Vega Seins
pub(crate) fn girr_vega_sens_weighted(_: &OCP) -> Expr {
    girr_vega_sens_weighted_05y().fill_null(0.)
        + girr_vega_sens_weighted_1y().fill_null(0.)
        + girr_vega_sens_weighted_3y().fill_null(0.)
        + girr_vega_sens_weighted_5y().fill_null(0.)
        + girr_vega_sens_weighted_10y().fill_null(0.)
}

/// Interm Result: GIRR Vega Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn girr_vega_sb(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

///calculate GIRR Vega Low Capital charge
pub(crate) fn girr_vega_charge_low(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Interm Result: GIRR Vega Low Kb
pub(crate) fn girr_vega_kb_low(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate GIRR Vega Medium Capital charge
pub(crate) fn girr_vega_charge_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Interm Result: GIRR Vega Medium Kb
pub(crate) fn girr_vega_kb_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate GIRR Vega Medium Capital charge
pub(crate) fn girr_vega_charge_high(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Interm Result: GIRR Vega Medium Kb
pub(crate) fn girr_vega_kb_high(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_vega_charge_distributor(
    op: &OCP,
    scenario: &'static ScenarioConfig,
    rtrn: ReturnMetric,
) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    let girr_vega_rho = get_optional_parameter_array(
        op,
        format!("girr_vega_rho{_suffix}").as_str(),
        &scenario.girr_vega_rho,
    );
    let girr_vega_gamma = get_optional_parameter(
        op,
        format!("girr_vega_gamma{_suffix}").as_str(),
        &scenario.girr_gamma,
    );

    let girr_vega_gamma_crr2_erm2 = get_optional_parameter(
        op,
        format!("girr_vega_gamma_erm2{_suffix}").as_str(),
        &scenario.girr_gamma_crr2_erm2,
    );
    let erm2ccys = get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_crr2);

    girr_vega_charge(
        girr_vega_rho,
        girr_vega_gamma,
        rtrn,
        juri,
        girr_vega_gamma_crr2_erm2,
        erm2ccys,
    )
}

fn girr_vega_charge(
    girr_vega_opt_rho: Array2<f64>,
    girr_gamma: f64,
    return_metric: ReturnMetric,
    juri: Jurisdiction,
    _erm2_gamma: f64,
    _erm2ccys: Vec<String>,
) -> Expr {
    apply_multiple(
        move |columns| {
            let df = df![
                "rcat" => columns[0].clone(),
                "rc" =>   columns[1].clone(),
                "b" =>    columns[2].clone(),
                "um" =>   columns[3].clone(),
                "y05" =>  columns[4].clone(),
                "y1" =>   columns[5].clone(),
                "y3" =>   columns[6].clone(),
                "y5" =>   columns[7].clone(),
                "y10" =>  columns[8].clone(),
                "weight"=>columns[9].clone(),
            ]?;

            let df = df
                .lazy()
                .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Vega"))))
                .groupby([col("b"), col("um")])
                .agg([
                    (col("y05") * col("weight")).sum(),
                    (col("y1") * col("weight")).sum(),
                    (col("y3") * col("weight")).sum(),
                    (col("y5") * col("weight")).sum(),
                    (col("y10") * col("weight")).sum(),
                ])
                .fill_null(lit::<f64>(0.))
                .collect()?;

            let res_len = columns[0].len();
            if df.height() == 0 {
                return Ok(Series::from_vec(
                    "res",
                    vec![0.; columns[0].len()] as Vec<f64>,
                ));
            };

            let part = df.partition_by(["b"])?;
            let res_buckets_kbs_sbs: Result<Vec<((&str, f64), f64)>> = part
                .par_iter()
                .map(|bdf| girr_vega_bucket_kb_sb(bdf, &girr_vega_opt_rho))
                .collect();

            let buckets_kbs_sbs = res_buckets_kbs_sbs?;
            let (buckets_kbs, sbs): (Vec<(&str, f64)>, Vec<f64>) =
                buckets_kbs_sbs.into_iter().unzip();
            let (_buckets, kbs): (Vec<&str>, Vec<f64>) = buckets_kbs.into_iter().unzip();

            // Early return Kb or Sb, ie the required metric
            match return_metric {
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

            // 325ag
            let mut gamma = match juri {
                #[cfg(feature = "CRR2")]
                Jurisdiction::CRR2 => build_girr_crr2_gamma(
                    &_buckets,
                    &_erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                    girr_gamma,
                    _erm2_gamma,
                ),
                _ => Array2::from_elem((kbs.len(), kbs.len()), girr_gamma),
            };
            let zeros = Array1::zeros(kbs.len());
            gamma.diag_mut().assign(&zeros);

            across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::DeltaVega)
        },
        &[
            col("RiskCategory"),
            col("RiskClass"),
            col("BucketBCBS"),
            col("GirrVegaUnderlyingMaturity"),
            col("Sensitivity_05Y"),
            col("Sensitivity_1Y"),
            col("Sensitivity_3Y"),
            col("Sensitivity_5Y"),
            col("Sensitivity_10Y"),
            col("SensWeights").arr().get(0),
        ],
        GetOutput::from_type(DataType::Float64),
    )
}

fn girr_vega_bucket_kb_sb<'a>(
    bucket_df: &'a DataFrame,
    girr_vega_rho: &Array2<f64>,
) -> Result<((&'a str, f64), f64)> {
    let bucket = unsafe { bucket_df["b"].utf8()?.get_unchecked(0).unwrap_or("Default") };

    // Extracting yield curves
    let yield_05um = girr_underlying_maturity_arr(bucket_df, "0.5Y", bucket)?;
    let yield_1um = girr_underlying_maturity_arr(bucket_df, "1Y", bucket)?;
    let yield_3um = girr_underlying_maturity_arr(bucket_df, "3Y", bucket)?;
    let yield_5um = girr_underlying_maturity_arr(bucket_df, "5Y", bucket)?;
    let yield_10um = girr_underlying_maturity_arr(bucket_df, "10Y", bucket)?;
    let infl = girr_underlying_maturity_arr(bucket_df, "Inflation", bucket)?;
    let xccy = girr_underlying_maturity_arr(bucket_df, "XCCY", bucket)?;

    let mut a = Array1::<f64>::uninit(
        yield_05um.len()
            + yield_1um.len()
            + yield_3um.len()
            + yield_5um.len()
            + yield_10um.len()
            + infl.len()
            + xccy.len(),
    );

    // better than concat and stack
    let mut i = 0usize;
    for arr in [
        yield_05um, yield_1um, yield_3um, yield_5um, yield_10um, infl, xccy,
    ] {
        let len = arr.len();
        let slice = a.slice_mut(s![i..i + len]);
        arr.move_into_uninit(slice);
        i += len;
    }

    let sens = unsafe { a.assume_init() };

    let a = sens.dot(girr_vega_rho);

    //21.4.4
    let kb = a.dot(&sens).max(0.).sqrt();

    //21.4.5.a
    let sb = sens.sum();

    Ok(((bucket, kb), sb))
}

/// Returns Array1 of shape 5 which represents 5 option mat tenors for a given
/// girr maturity
pub(crate) fn girr_underlying_maturity_arr(
    df: &DataFrame,
    mat: &str,
    _: &str,
) -> Result<Array1<f64>> {
    let mask = df["um"].equal(mat)?;
    Ok(df
        .filter(&mask)?
        .select(["y05", "y1", "y3", "y5", "y10"])?
        .to_ndarray::<Float64Type>()?
        .into_shape(5)
        .unwrap_or_else(|_| Array1::<f64>::zeros(5)))
}

pub(crate) fn girr_vega_rho() -> Array2<f64> {
    let base = option_maturity_rho();
    let mut arr = Array2::<f64>::uninit((35, 35));
    arr.exact_chunks_mut((5, 5))
        .into_iter()
        .enumerate()
        //.par_bridge()
        .for_each(|(i, chunk)| {
            //we have total 7(chunks per row)*7(chunks per col) = 49 chunks
            let row_id = i / 7; //eg 27usize/5usize = 5usize
            let col_id = i % 7; //eg 27usize % 5usize = 2usize
            if row_id == col_id {
                base.to_owned().move_into_uninit(chunk)
            } else if (row_id == 6) | (col_id == 6) {
                (&base * 0.).move_into_uninit(chunk)
            } else if (row_id == 5) | (col_id == 5) {
                (&base * 0.4).move_into_uninit(chunk)
            } else {
                let mult = unsafe { *base.uget((row_id, col_id)) };
                (&base * mult).move_into_uninit(chunk)
            }
        });
    let mut res: Array2<f64>;
    unsafe {
        res = arr.assume_init();
    }
    // 21.93 the min function
    res.map_inplace(|x| *x = f64::min(*x, 1.));
    res
}
/// Returns max of three scenarios
/// 
/// !Note This is not a real measure, as MAX should be taken as
/// MAX(ir_delta_low+ir_vega_low+eq_curv_low, ..._medium, ..._high).
/// This is for convienience view only.
fn girr_vega_max(op: &OCP) -> Expr {
    max_exprs(&[girr_vega_charge_low(op), girr_vega_charge_medium(op), girr_vega_charge_high(op)])
}

/// Exporting Measures
pub(crate) fn girr_vega_measures() -> Vec<Measure<'static>> {
    vec![
        Measure {
            name: "GIRR_VegaSens".to_string(),
            calculator: Box::new(total_ir_vega_sens),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaSens_Weighted".to_string(),
            calculator: Box::new(girr_vega_sens_weighted),
            aggregation: None,
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaSb".to_string(),
            calculator: Box::new(girr_vega_sb),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaCharge_Low".to_string(),
            calculator: Box::new(girr_vega_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaKb_Low".to_string(),
            calculator: Box::new(girr_vega_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaCharge_Medium".to_string(),
            calculator: Box::new(girr_vega_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaKb_Medium".to_string(),
            calculator: Box::new(girr_vega_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaCharge_High".to_string(),
            calculator: Box::new(girr_vega_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaKb_High".to_string(),
            calculator: Box::new(girr_vega_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
        Measure {
            name: "GIRR_VegaCharge_MAX".to_string(),
            calculator: Box::new(girr_vega_max),
            aggregation: Some("first"),
            precomputefilter: Some(
                col("RiskCategory")
                    .eq(lit("Vega"))
                    .and(col("RiskClass").eq(lit("GIRR"))),
            ),
        },
    ]
}