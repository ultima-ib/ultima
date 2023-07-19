//! Common functionality associated with Curvature Calculations

#![allow(clippy::type_complexity, clippy::unnecessary_lazy_evaluations)]
//#![allow(clippy::unnecessary_lazy_evaluations)]

use std::sync::{Arc, Mutex};

use ndarray::Array2;
use polars::lazy::dsl::{apply_multiple, col, lit, Expr, GetOutput};
use polars::prelude::{AnyValue, ChunkAgg, ChunkSet, DataType, PolarsError};
use polars::series::{ChunkCompare, IntoSeries, Series};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use ultibi::{DataFrame, PolarsResult};

use crate::prelude::var_covar_sum_single;
use crate::sbm::common::*;

/// Filtering total delta on risk class and PnL Up or PnL Down is not null
/// giving us Curvature Delta
fn curv_delta(rc: &'static str, risk: Expr) -> Expr {
    apply_multiple(
        move |columns| {
            //RiskClass
            let mask = columns[0].utf8()?.equal(rc);

            let mask1 = columns[1].f64()?.is_not_null();

            let mask2 = columns[2].f64()?.is_not_null();

            let pnl_up_or_down_is_not_null = mask1 | mask2;

            let risk_filtered = columns[3]
                .f64()?
                .set(&!(mask & pnl_up_or_down_is_not_null), None)?;

            Ok(Some(risk_filtered.into_series()))
        },
        &[col("RiskClass"), col("PnL_Up"), col("PnL_Down"), risk],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

pub(crate) fn total_sens_curv_weighted() -> Expr {
    total_delta_sens() * col("CurvatureRiskWeight")
}

/// CSR
pub(crate) fn cvr_up_5() -> Expr {
    lit::<f64>(0.) - (col("PnL_Up") - total_vega_curv_sens() * col("CurvatureRiskWeight"))
}

pub(crate) fn cvr_down_5() -> Expr {
    lit::<f64>(0.) - (col("PnL_Down") + total_vega_curv_sens() * col("CurvatureRiskWeight"))
}

/// Commodity , GIRR
/// TODO GIRR case - we don't need to include SensitivitySpot(however, GIRR Yield SensitivitySpot shouldn't be provided in the first place)
pub(crate) fn cvr_up() -> Expr {
    lit::<f64>(0.) - (col("PnL_Up") - total_sens_curv_weighted())
}

pub(crate) fn cvr_down() -> Expr {
    lit::<f64>(0.) - (col("PnL_Down") + total_sens_curv_weighted())
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
/// TODO remove cast
/// https://github.com/pola-rs/polars/issues/4326
pub(crate) fn spot_sens_curv_weighted() -> Expr {
    col("SensitivitySpot").fill_null(0.) * col("CurvatureRiskWeight")
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
pub(crate) fn cvr_up_spot() -> Expr {
    lit::<f64>(0.) - (col("PnL_Up") - spot_sens_curv_weighted())
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
pub(crate) fn cvr_down_spot() -> Expr {
    lit::<f64>(0.) - (col("PnL_Down") + spot_sens_curv_weighted())
}

pub(crate) fn rc_cvr(rc: &'static str, dir: Cvr) -> Expr {
    let cvr = match dir {
        Cvr::Up => cvr_up(),
        Cvr::Down => cvr_down(),
    };
    rc_sens(rc, cvr)
}
pub(crate) fn rc_cvr_spot(rc: &'static str, dir: Cvr) -> Expr {
    let cvr = match dir {
        Cvr::Up => cvr_up_spot(),
        Cvr::Down => cvr_down_spot(),
    };
    rc_sens(rc, cvr)
}

pub(crate) fn rc_cvr_5(rc: &'static str, dir: Cvr) -> Expr {
    let cvr = match dir {
        Cvr::Up => cvr_up_5(),
        Cvr::Down => cvr_down_5(),
    };
    rc_sens(rc, cvr)
}

pub(crate) fn curv_delta_total(rc: &'static str) -> Expr {
    curv_delta(rc, total_delta_sens())
}

pub(crate) fn curv_delta_spot(rc: &'static str) -> Expr {
    curv_delta(rc, col("SensitivitySpot"))
}

/// 5 tenors only
pub(crate) fn curv_delta_5(rc: &'static str) -> Expr {
    curv_delta(rc, total_vega_curv_sens())
}

pub(crate) enum Cvr {
    Up,
    Down,
}
///21.5.3.b
pub(crate) fn phi(sbs: &Vec<f64>) -> Array2<f64> {
    let mut arr = Array2::ones((sbs.len(), sbs.len()));
    let mut tmp: Vec<usize> = Vec::with_capacity(sbs.len()); // To keep track on negative Sbs
    for (i, v) in sbs.iter().enumerate() {
        if *v < 0. {
            for t in &tmp {
                unsafe { *arr.uget_mut((i, *t)) = 0. };
                unsafe { *arr.uget_mut((*t, i)) = 0. };
            }
            tmp.push(i);
        }
    }
    arr
}

/// Calculates simple Kb plus or minus, IE only one cvr_up/down per bucket(and therefore Rho is 0)
///
/// For multiple buckets at the same time.
///
/// Used for FX, Girr curvature.
pub(crate) fn kb_plus_minus_simple(cvr_up_or_down: &Series) -> PolarsResult<Vec<f64>> {
    Ok(cvr_up_or_down
        .f64()?
        .into_iter()
        .map(|cvr|
        // No need to ^2 and SQRT since below is just one positive(or 0) number
        f64::max(cvr.unwrap_or_else(||0.), 0.))
        .collect())
}

/// calculates kb plus, kb minus, sb, kb per bucket simultaniously
/// * `df` expected to have columns "b", "cvr_up", "cvr_down"
pub(crate) fn curvature_kb_plus_minus(
    df: DataFrame,
    //n_buckets: usize,
    bucket_rho: &[f64],
    special_bucket: Option<usize>,
) -> PolarsResult<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let n_buckets = bucket_rho.len();

    let mut res_kb_cvr: Vec<(PolarsResult<(f64, f64)>, PolarsResult<(f64, f64)>)> =
        Vec::with_capacity(n_buckets);
    //let mut res_kbminus_cvrdown: Vec<Result<(f64, f64)>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets {
        res_kb_cvr.push((Ok((0., 0.)), Ok((0., 0.))))
    }

    let arc_mtx_kbpm_cvr = std::sync::Arc::new(Mutex::new(res_kb_cvr));

    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    df.partition_by(["b"], true)?
        .par_iter()
        .for_each(|bucket_df| {
            // Ok to go unsafe here becaause we validate length in [equity_delta_charge_distributor]
            let b_as_idx_plus_1 = unsafe { bucket_df["b"].get_unchecked(0) };
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st) => st.parse::<usize>().ok().and_then(|b_id| {
                    if b_id <= n_buckets {
                        Some(b_id)
                    } else {
                        None
                    }
                }),
                _ => None,
            };

            if let Some(b_as_idx_plus_1) = b_as_idx_plus_1 {
                let is_special_bucket = matches!(special_bucket, Some(b) if b == b_as_idx_plus_1);

                let rho = bucket_rho[b_as_idx_plus_1 - 1];
                // CALCULATE Kb Sb for a bucket
                let buck_kb_plus_cvr_up_sum =
                    kb_plus_minus(&bucket_df["cvr_up"], rho, is_special_bucket);
                let buck_kb_minus_cvr_down_sum =
                    kb_plus_minus(&bucket_df["cvr_down"], rho, is_special_bucket);
                let res = (buck_kb_plus_cvr_up_sum, buck_kb_minus_cvr_down_sum);
                let mut r = arc_mtx_kbpm_cvr.lock().unwrap(); //[b_as_idx_plus_1-1];// = res;
                r[b_as_idx_plus_1 - 1] = res;
            }
        });
    //Result<Vec<(f64, f64)>>
    let (res_kbp_cvrup, res_kbm_cvrdown): (
        Vec<PolarsResult<(f64, f64)>>,
        Vec<PolarsResult<(f64, f64)>>,
    ) = Arc::try_unwrap(arc_mtx_kbpm_cvr)
        .map_err(|_| PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_| PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .into_iter()
        .unzip();

    let res_kbp_cvrup: PolarsResult<Vec<(f64, f64)>> = res_kbp_cvrup.into_iter().collect();
    let res_kbm_cvrdown: PolarsResult<Vec<(f64, f64)>> = res_kbm_cvrdown.into_iter().collect();
    Ok((res_kbp_cvrup?, res_kbm_cvrdown?))
}

/// ( Curvature Kb Plus/Minus , CVR_UP_DOWN Sum )
/// * `special_bucket` indicates if CVR Up/Down to be calculated accordingly to 21.79
pub(crate) fn kb_plus_minus(
    cvr_up_or_down: &Series,
    rho: f64,
    special_bucket: bool,
) -> PolarsResult<(f64, f64)> {
    // If special bucket, we simply sum positive values for Kb puls/minus
    if special_bucket {
        let sum_positive: f64 = cvr_up_or_down
            .f64()?
            .into_no_null_iter()
            .filter(|x| x.is_sign_positive())
            .sum();
        let sum = cvr_up_or_down.f64()?.sum().unwrap_or_default();
        return Ok((sum_positive, sum));
    }
    //First, calculate as if normal var covar sum
    let (sum, full) = var_covar_sum_single(cvr_up_or_down, rho)?;
    //Then, subtract squares where max(CVR,0)^2 would be 0 (ie squares of negative numbers)
    //And Correlations of negative numbers. For this first get srs of negative items
    let neg_srs = cvr_up_or_down
        .f64()?
        .into_iter()
        .filter(|o| {
            if let Some(x) = o {
                x.is_sign_negative()
            } else {
                false
            }
        })
        .collect::<Series>();
    let (_, neg) = var_covar_sum_single(&neg_srs, rho)?;

    Ok(((full - neg).max(0.).sqrt(), sum))
}

/// Computes Kb, Sb from kb_plus, kb_minus, sum_cvr_up, sum_cvr_down.
/// TODO take cre of special bucket
pub(crate) fn kbs_sbs_curvature<I>(
    kb_plus: Vec<f64>,
    kb_minus: Vec<f64>,
    cvr_up: I,
    cvr_down: I,
) -> PolarsResult<(Vec<f64>, Vec<f64>)>
where
    I: Iterator<Item = Option<f64>>,
{
    let (kbs, sbs): (Vec<f64>, Vec<f64>) = kb_plus
        .into_iter()
        .zip(kb_minus.into_iter())
        .zip(cvr_up)
        .zip(cvr_down)
        .map(|(((kb_p, kb_m), cv_up), cv_down)| {
            if kb_p > kb_m {
                (kb_p, cv_up.unwrap_or_else(|| 0.))
            } else if kb_m > kb_p {
                (kb_m, cv_down.unwrap_or_else(|| 0.))
            } else {
                // 21.5.3.a.iii
                if cv_up > cv_down {
                    (kb_p, cv_up.unwrap_or_else(|| 0.))
                } else {
                    (kb_m, cv_down.unwrap_or_else(|| 0.))
                }
            }
        })
        .unzip();

    Ok((kbs, sbs))
}
