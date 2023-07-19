//#![allow(clippy::unnecessary_lazy_evaluations)]

use ultibi::polars::prelude::IndexOrder;
use ultibi::prelude::*;

use ndarray::{s, Array1, Array2, ArrayView1, Axis, Zip};
use ultibi::polars::export::num::Signed;
use ultibi::polars::prelude::{
    apply_multiple, AnyValue, ChunkAgg, ChunkCompare, ChunkSet, DataType, FillNullStrategy,
    Float64Type, GetOutput, IntoSeries, NamedFrom, NumOpsDispatch, PolarsError, Series, TakeRandom,
};

use rayon::{
    iter::{ParallelBridge, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use std::mem::MaybeUninit as MU;
use std::sync::{Arc, Mutex};

use crate::prelude::{RhoOverwrite, RhoType};

/// Sum of all delta sensis, from spot to 30Y tenor
/// In practice should be used only with filter on RiskClass
/// as combining FX and IR sensis is meaningless
pub fn total_delta_sens() -> Expr {
    // When adding Exprs NULLs have to be filled Otherwise returns NULL
    col("SensitivitySpot").fill_null(0.)
        + col("Sensitivity_025Y").fill_null(0.)
        + col("Sensitivity_05Y").fill_null(0.)
        + col("Sensitivity_1Y").fill_null(0.)
        + col("Sensitivity_2Y").fill_null(0.)
        + col("Sensitivity_3Y").fill_null(0.)
        + col("Sensitivity_5Y").fill_null(0.)
        + col("Sensitivity_10Y").fill_null(0.)
        + col("Sensitivity_15Y").fill_null(0.)
        + col("Sensitivity_20Y").fill_null(0.)
        + col("Sensitivity_30Y").fill_null(0.)
}

/// CSR, Vega
pub fn total_vega_curv_sens() -> Expr {
    // When adding Exprs NULLs have to be filled
    // Otherwise returns NULL
    col("Sensitivity_05Y").fill_null(0.)
        + col("Sensitivity_1Y").fill_null(0.)
        + col("Sensitivity_3Y").fill_null(0.)
        + col("Sensitivity_5Y").fill_null(0.)
        + col("Sensitivity_10Y").fill_null(0.)
}

/// Filtering risk on rcat and risk class
pub fn rc_rcat_sens(rcat: &'static str, rc: &'static str, risk: Expr) -> Expr {
    apply_multiple(
        move |columns| {
            //RiskClass
            let mask = columns[0].utf8()?.equal(rc);
            //RiskCategory
            let mask1 = columns[1].utf8()?.equal(rcat);

            let risk_filtered = columns[2].f64()?.set(&!(mask & mask1), None)?;

            Ok(Some(risk_filtered.into_series()))
        },
        &[col("RiskClass"), col("RiskCategory"), risk],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

/// Filtering risk on risk class
pub fn rc_sens(rc: &'static str, risk: Expr) -> Expr {
    apply_multiple(
        move |columns| {
            //RiskClass
            let mask = columns[0].utf8()?.equal(rc);

            let risk_filtered = columns[1].f64()?.set(&!mask, None)?;

            Ok(Some(risk_filtered.into_series()))
        },
        &[col("RiskClass"), risk],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

/// Helper function to derive weighted delta,
/// per tenor, per risk class, per risk Category
pub fn rc_tenor_weighted_sens(
    rcat: &'static str,
    rc: &'static str,
    delta_tenor: &str,
    weights_col: &str,
    weight_idx: i64,
) -> Expr {
    apply_multiple(
        move |columns| {
            //RiskClass
            let mask = columns[0].utf8()?.equal(rc);
            //RiskCategory
            let mask1 = columns[3].utf8()?.equal(rcat);

            let delta = columns[1].f64()?.set(&!(mask & mask1), None)?;

            let x = delta.multiply(&columns[2])?;
            Ok(Some(x))
        },
        &[
            col("RiskClass"),
            col(delta_tenor),
            col(weights_col).list().get(lit(weight_idx)),
            col("RiskCategory"),
        ],
        GetOutput::from_type(DataType::Float64),
        false,
    )
}

///makes sence at RiskClass-Bucket view
pub fn sens_weights(_: &CPM) -> PolarsResult<Expr> {
    Ok(col("SensWeights"))
}

pub(crate) fn across_bucket_agg<I: IntoIterator<Item = f64>>(
    kbs: I,
    sbs: I,
    gamma: &Array2<f64>,
    _: usize,
    sbm_type: SBMChargeType,
) -> PolarsResult<Option<Series>> {
    let kbs_arr = Array1::from_iter(kbs);
    let sbs_arr = Array1::from_iter(sbs);

    //21.4.5 sum{ sum {gamma*s_b*s_c} }
    let a = sbs_arr.dot(gamma);
    let b = a.dot(&sbs_arr);

    //21.4.5 sum{Kb^2}
    let c = kbs_arr.dot(&kbs_arr);

    let sum = c + b;

    let res = match sbm_type {
        SBMChargeType::DeltaVega => {
            if sum < 0. {
                //21.4.5.b
                let sbs_alt = alt_sbs(sbs_arr.view(), kbs_arr.view());
                //now recalculate capital charge with alternative sb
                //21.4.5 sum{ sum {gamma*s_b*s_c} }
                let a = sbs_alt.dot(gamma);
                let b = a.dot(&sbs_alt);
                //21.4.5 sum{K-b^2}
                let c = kbs_arr.dot(&kbs_arr);
                let sum = c + b;
                sum.sqrt()
            } else {
                sum.sqrt()
            }
        }

        SBMChargeType::Curvature => f64::max(sum, 0.).sqrt(),
    };

    // The function is supposed to return a series of same len as the input, hence we broadcast the result
    //let res_arr = Array::from_elem(res_len, res);
    // if option panics on .unwrap() implement match and use .iter() and then Series from iter
    Ok(Some(Series::new("Res", [res])))
    // Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
}

pub(crate) enum SBMChargeType {
    DeltaVega,
    Curvature,
}

pub(crate) fn alt_sbs(sbs_arr: ArrayView1<f64>, kbs_arr: ArrayView1<f64>) -> Array1<f64> {
    //21.4.5.b
    let mut sbs_alt = Array1::<f64>::zeros(kbs_arr.raw_dim());
    Zip::from(&mut sbs_alt)
        .and(sbs_arr)
        .and(kbs_arr)
        .par_for_each(|alt, &sb, &kb| {
            let _min = sb.min(kb);
            *alt = _min.max(-kb);
        });
    sbs_alt
}

/// Column "b" expected. Value in col "b" is expected to be parsable into usize
///
/// Internally calls [bucket_kb_sb_onsq].
///
/// Commodity and CSR CTP. Common way of calculating Kbs and Sbs for all buckets, where no optimisation is possible
/// due to many risk factor types (location/tranche).
#[allow(clippy::too_many_arguments)]
pub(crate) fn all_kbs_sbs_onsq<F>(
    df: DataFrame,
    tenor_col: &str,
    rho_diff_tenor: f64,
    name_col: &str,
    bucket_rho_diff_rf: &[f64],
    basis_col: &str,
    rho_diff_rft: f64,
    risk_col: &str,
    scenario_fn: F,
    special_bucket: Option<usize>,
    rho_overwrite: &Option<RhoOverwrite>,
) -> PolarsResult<Vec<(f64, f64)>>
where
    F: Fn(f64) -> f64 + Sync + Send + Copy,
{
    let n_buckets = bucket_rho_diff_rf.len();
    let mut reskbs_sbs: Vec<PolarsResult<(f64, f64)>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets {
        reskbs_sbs.push(Ok((0., 0.)))
    }

    let arc_mtx = std::sync::Arc::new(Mutex::new(reskbs_sbs));
    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    df.partition_by(["b"], true)?
        .par_iter()
        .for_each(|bucket_df| {
            // Safety: since partition, we must have at least one member
            let b_as_idx_plus_1 = unsafe { bucket_df["b"].get_unchecked(0) };
            // validating also bucket is not greater than max index of bucket_rho_diff_rf
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st) => st.parse::<usize>().ok().and_then(|b_id| {
                    if (1..=n_buckets).contains(&b_id) {
                        Some(b_id)
                    } else {
                        None
                    }
                }),

                _ => None,
            };

            // CALCULATE Kb Sb for a bucket
            if let Some(b_as_idx_plus_1) = b_as_idx_plus_1 {
                // Above we check len of bucket_rho_diff_rf via n_buckets, so we won't panic here
                let name_rho = bucket_rho_diff_rf[b_as_idx_plus_1 - 1];

                // CALCULATE Kb Sb for a bucket
                let is_special_bucket = Some(b_as_idx_plus_1) == special_bucket;
                let a = bucket_kb_sb_onsq(
                    bucket_df,
                    tenor_col,
                    rho_diff_tenor,
                    name_col,
                    name_rho,
                    basis_col,
                    rho_diff_rft,
                    risk_col,
                    scenario_fn,
                    is_special_bucket,
                    rho_overwrite,
                );

                let mut res = arc_mtx.lock().unwrap();
                res[b_as_idx_plus_1 - 1] = a;
            }
        });
    let reskbs_sbs: PolarsResult<Vec<(f64, f64)>> = Arc::try_unwrap(arc_mtx)
        .map_err(|_| PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_| PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .into_iter()
        .collect();
    reskbs_sbs
}

/// Commodity and CSR Sec Non CTP have potentially unlimited number of locations/tranches.
///
/// Hence we can't go with any of the optimizations. Have to be computed in O(N^2)
#[allow(clippy::too_many_arguments, clippy::if_same_then_else)]
fn bucket_kb_sb_onsq<F>(
    df: &DataFrame,
    tenor_col: &str,
    rho_diff_tenor: f64,
    name_col: &str,
    rho_diff_name: f64,
    basis_col: &str,
    rho_diff_rft: f64,
    risk_col: &str,
    scenario_fn: F,
    is_special_bucket: bool,
    rho_overwrite: &Option<RhoOverwrite>,
) -> PolarsResult<(f64, f64)>
where
    F: Fn(f64) -> f64 + Sync + Send,
{
    let risk_chunked = df[risk_col].f64()?;
    let sb = risk_chunked.sum().unwrap_or_default();
    if is_special_bucket {
        let res = risk_chunked
            .into_no_null_iter()
            .map(|x| x.abs())
            .sum::<f64>();
        return Ok((res, sb));
    }
    let tenor_chunked = df[tenor_col].utf8()?;
    let name_chunked = df[name_col].utf8()?;
    let basis_chunked = df[basis_col].utf8()?;
    // If special rho was provided - unpack
    let special_col = if let Some(sp_rho) = rho_overwrite {
        Some((
            df.column(&sp_rho.column)?.utf8()?, //0
            &sp_rho.col_equals,                 //1
            sp_rho.oneway,                      //2
            sp_rho.value,                       //3
            sp_rho.rhotype,                     //4
        ))
    } else {
        None
    };
    //let special_rho_unpacked =

    let mut res = 0.;
    let it = tenor_chunked
        .into_iter()
        .zip(name_chunked.into_iter())
        .zip(basis_chunked.into_iter())
        .zip(risk_chunked.into_iter());

    for (i, (((tenor, name), basis), risk)) in it.enumerate() {
        if let Some(risk) = risk {
            res += risk.powi(2);

            let it2 = tenor_chunked
                .into_iter()
                .zip(name_chunked.into_iter())
                .zip(basis_chunked.into_iter())
                .zip(risk_chunked.into_iter());

            for (j, (((tenor2, name2), basis2), risk2)) in it2.enumerate().skip(i + 1) {
                if let Some(risk2) = risk2 {
                    let mut rho = 1.;
                    if tenor != tenor2 {
                        // TODO bring it out as separate function and extend to Name and Basis Rhos
                        // Extending to name - shouldn't be a problem since special column will act as bucket
                        let _rho_diff_tenor = match special_col {
                            //special case covering special rho
                            Some((sp_col, col_value, oneway, rho_override, RhoType::Tenor)) => {
                                let a = sp_col.get(i);
                                let b = sp_col.get(j);
                                if oneway
                                    & ((a == Some(col_value.as_str()))
                                        | (b == Some(col_value.as_str())))
                                {
                                    rho_override
                                } else if !oneway
                                    & ((a == Some(col_value.as_str()))
                                        & (b == Some(col_value.as_str())))
                                {
                                    rho_override
                                } else {
                                    rho_diff_tenor
                                }
                            }
                            _ => rho_diff_tenor,
                        };
                        rho *= _rho_diff_tenor;
                    }
                    if name != name2 {
                        rho *= rho_diff_name
                    }
                    if basis != basis2 {
                        rho *= rho_diff_rft
                    }
                    res += 2f64 * scenario_fn(rho) * risk * risk2
                }
            }
        }
    }
    Ok((res.max(0.).sqrt(), sb))
}

/// 21.93
pub fn option_maturity_rho() -> Array2<f64> {
    let mut option_maturity_rho = Array2::<f64>::zeros((5, 5));
    let tenors = [0.5, 1., 3., 5., 10.];

    for ((row, col), val) in option_maturity_rho.indexed_iter_mut() {
        let tr = tenors[row];
        let tc = tenors[col];
        *val = vega_rho_element(tr, tc);
    }

    option_maturity_rho
}
/// 21.93
fn vega_rho_element(m1: f64, m2: f64) -> f64 {
    let alpha = 0.01;
    f64::exp(-alpha * f64::abs(m1 - m2) / f64::min(m1, m2))
}

/// Equity and CSRnonSec and CSR sec CTP share common approach
/// They have limited number of buckets.
/// They have 2 variants for RFT, and many different names
/// The difference between them is only in number of tenors
///
/// *df is expected to have "b" column representing bucket
#[allow(clippy::too_many_arguments)]
pub(crate) fn all_kbs_sbs_two_types<F>(
    df: DataFrame,
    n_buckets: usize,
    bucket_rho_diff_rf: &[f64],
    rho_base_diff_rft_or_loc: f64,
    scenario_fn: F,
    special_bucket: Option<usize>,
    cols_by_tenor: &[(&str, &str)],
    dtenor: Option<f64>,
) -> PolarsResult<Vec<(f64, f64)>>
where
    F: Fn(f64) -> f64 + Sync + Send + Copy,
{
    let mut reskbs_sbs: Vec<PolarsResult<(f64, f64)>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets {
        reskbs_sbs.push(Ok((0., 0.)))
    }

    let arc_mtx = std::sync::Arc::new(Mutex::new(reskbs_sbs));
    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    df.fill_null(FillNullStrategy::Zero)?
        .partition_by(["b"], true)?
        .par_iter()
        .for_each(|bucket_df| {
            // Ok to go unsafe here becaause we validate length in [equity_delta_charge_distributor]
            let b_as_idx_plus_1 = unsafe { bucket_df["b"].get_unchecked(0) };
            // validating also bucket is not greater than max index of bucket_rho_diff_rf
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
            // CALCULATE Kb Sb for a bucket
            if let Some(b_as_idx_plus_1) = b_as_idx_plus_1 {
                let buck_kb_sb = bucket_kb_sb_two_types(
                    bucket_df,
                    b_as_idx_plus_1,
                    special_bucket,
                    bucket_rho_diff_rf,
                    rho_base_diff_rft_or_loc,
                    scenario_fn,
                    cols_by_tenor,
                    dtenor,
                );
                let _idx = b_as_idx_plus_1 - 1;
                arc_mtx.lock().unwrap()[_idx] = buck_kb_sb;
            }
        });
    let reskbs_sbs: PolarsResult<Vec<(f64, f64)>> = Arc::try_unwrap(arc_mtx)
        .map_err(|_| PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_| PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .into_iter()
        .collect();
    reskbs_sbs
}

/// This function assumes two RFTs
/// * `df` - A "pivoted" Dataframe. Rows are names(RFs), columns are 2(for each RFT) x ntenors. Expecting no NULLs
/// * `tenor_cols` , all columns are expected to .f64(), otherwise we will panic
#[allow(clippy::too_many_arguments)]
fn bucket_kb_sb_two_types<F>(
    df: &DataFrame,
    bucket_id: usize,
    special_bucket: Option<usize>,
    rho_diff_rf_bucket: &[f64],
    rho_diff_rft: f64,
    scenario_fn: F,
    cols_by_tenor: &[(&str, &str)],
    dtenor: Option<f64>,
) -> PolarsResult<(f64, f64)>
where
    F: Fn(f64) -> f64 + Sync + Send,
{
    // 21.56 First, take care of the special bucket
    match special_bucket {
        Some(x) if x == bucket_id => {
            let mut abs_sum = 0.;
            for (c1, c2) in cols_by_tenor.iter() {
                abs_sum += df[*c1]
                    .f64()?
                    .into_no_null_iter()
                    .map(|x| x.abs())
                    .sum::<f64>()
                    + df[*c2]
                        .f64()?
                        .into_no_null_iter()
                        .map(|x| x.abs())
                        .sum::<f64>();
            }
            return Ok((abs_sum, abs_sum));
        }
        _ => (),
    };

    let rho_name_bucket = rho_diff_rf_bucket.get(bucket_id - 1).unwrap_or(&0.);
    let rho_case1 = scenario_fn(*rho_name_bucket); //Diff name, same type
    let rho_case2 = scenario_fn(rho_diff_rft); //Diff type, same name
    let rho_case3 = scenario_fn(rho_name_bucket * rho_diff_rft); //Diff name, diff type

    let mut sb = 0.; //this is Sb
    let mut var_covar_sum = 0.; // this is pre Kb(var-covar sum) for a single tenor
    let mut cross_tenor = 0.;

    for (t, (c1, c2)) in cols_by_tenor.iter().enumerate() {
        //println!("{t}");
        let (pre_kb, pre_sb) =
            var_covar_sum_fn(&df[*c1], &df[*c2], rho_case1, rho_case2, rho_case3);
        sb += pre_sb;
        var_covar_sum += pre_kb;
        // Now, if there are any other tenors, we need to account for them
        if cols_by_tenor[(t + 1)..].is_empty() {
            continue;
        };
        // First, check dtenor was provided
        if let Some(dt) = dtenor {
            let rho_case4 = scenario_fn(dt);
            let rho_case5 = scenario_fn(dt * rho_diff_rft);
            let rho_case6 = scenario_fn(dt * rho_name_bucket);
            let rho_case7 = scenario_fn(dt * rho_diff_rft * rho_name_bucket);
            let mut arr_tenor = df
                .select([*c1, *c2])?
                .to_ndarray::<Float64Type>(IndexOrder::Fortran)?; // Nulls must've been filled
            let dim = arr_tenor.raw_dim();

            let mut next_tenors_sum = Array2::<f64>::zeros(dim);
            for (c3, c4) in cols_by_tenor[(t + 1)..].iter() {
                let next_tenor = df
                    .select([*c3, *c4])?
                    //.fill_null(FillNullStrategy::Zero)?
                    .to_ndarray::<Float64Type>(IndexOrder::Fortran)?; // Nulls must've been filled
                next_tenors_sum = next_tenors_sum + next_tenor;
            }
            let type0_sum = next_tenors_sum.slice(s![.., 0]).sum();
            let type1_sum = next_tenors_sum.slice(s![.., 1]).sum();
            arr_tenor
                .indexed_iter_mut()
                .par_bridge()
                .for_each(|((i, j), v)| {
                    let anti_j = usize::from(j == 0); // j is either 0 or 1
                    let same_name_same_type = unsafe { next_tenors_sum.uget((i, j)) };
                    let same_name_diff_type = unsafe { next_tenors_sum.uget((i, anti_j)) };
                    let same_type_sum = if j == 0 { type0_sum } else { type1_sum };
                    let diff_type_sum = if j == 1 { type0_sum } else { type1_sum };

                    let next_tenors_weighted = same_type_sum * rho_case6
                        - rho_case6 * same_name_same_type
                        + rho_case4 * same_name_same_type
                        + diff_type_sum * rho_case7
                        - rho_case7 * same_name_diff_type
                        + rho_case5 * same_name_diff_type;

                    let a = 2. * (*v) * next_tenors_weighted;
                    *v = a;
                });

            cross_tenor += arr_tenor.sum();
        }
    }

    let kb = (var_covar_sum + cross_tenor).max(0.).sqrt();
    Ok((kb, sb))
}

/// Calculates Var-Covar Matrix in O(N) for !two distinct risk types!
/// * `srs1` and `srs2` are expected to be ".f64()"
/// * `rho_case1` Diff name, same type (diff name, non-nn)
/// * `rho_case2` Same name, diff type (same name, non-nn)
/// * `rho_case3` Diff name, diff type (_, nn)
pub(crate) fn var_covar_sum_fn(
    srs1: &Series,
    srs2: &Series,
    rho_case1: f64,
    rho_case2: f64,
    rho_case3: f64,
) -> (f64, f64) {
    //let (spot_sum, spot_f1) = var_covar_sum_single(srs1, rho_case1);
    //let (repo_sum, repo_f1) = var_covar_sum_single(srs2, rho_case1);

    let mut spot_sum = 0.;
    let mut spot_sum_of_sq = 0.;

    let mut repo_sum = 0.;
    let mut repo_sum_of_sq = 0.;

    let mut spot_times_repo_sum = 0.;

    srs1.f64()
        .unwrap()
        .into_iter()
        .zip(srs2.f64().unwrap().into_iter())
        .for_each(|(x, y)| {
            let x = x.unwrap_or_default();
            let y = y.unwrap_or_default();
            spot_sum += x;
            spot_sum_of_sq += x.powi(2);
            repo_sum += y;
            repo_sum_of_sq += y.powi(2);
            spot_times_repo_sum += x * y;
        });
    let pre_sb = repo_sum + spot_sum;

    let formula1 = spot_sum_of_sq
        + repo_sum_of_sq
        + rho_case1 * (spot_sum.powi(2) - spot_sum_of_sq + repo_sum.powi(2) - repo_sum_of_sq);

    let formula2_pt1 = spot_sum * repo_sum * rho_case3;

    let formula2_pt2 = rho_case2 * spot_times_repo_sum;
    let formula2_pt3 = -rho_case3 * spot_times_repo_sum;
    let formula2 = formula2_pt1 + formula2_pt2 + formula2_pt3;

    let pre_kb = formula1 + 2f64 * formula2;

    (pre_kb, pre_sb)
}

/// Rho represents rho between risk factors where name/rf is different
///
/// Returns: (sum(for Sb), formula1(Kb) )
pub(crate) fn var_covar_sum_single(srs: &Series, rho: f64) -> PolarsResult<(f64, f64)> {
    let mut sum_of_sq = 0.;
    let mut sum = 0.;
    srs.f64()?.into_no_null_iter().for_each(|x| {
        sum_of_sq += x.powi(2);
        sum += x;
    });

    let f1 = sum_of_sq - rho * sum_of_sq + rho * sum.powi(2);

    Ok((sum, f1))
}

/// Vega has Tenors and single type
pub(crate) fn all_kbs_sbs_single_type<F>(
    df: DataFrame,
    rho_same_curve: &Array2<f64>,
    rho_diff_curve: &[f64],
    scenario_fn: F,
    columns: &[&'static str],
    special_bucket: Option<&'static str>,
) -> PolarsResult<Vec<(f64, f64)>>
where
    F: Fn(f64) -> f64 + Sync + Send + Copy,
{
    let n_buckets = rho_diff_curve.len();

    let mut reskbs_sbs: Vec<PolarsResult<(String, (f64, f64))>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets {
        reskbs_sbs.push(Ok(("".to_string(), (0., 0.))))
    }

    let arc_mtx = std::sync::Arc::new(Mutex::new(reskbs_sbs));
    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    df.partition_by(["b"], true)?
        .par_iter()
        .for_each(|bucket_df| {
            // Safety: since we are in partition, bucket_df["b"] has at least one element
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

            // For example if CSR BCBS bucket is 19, then we would have None here
            // Now, if b_as_idx_plus_1 is None then we simply do nothing
            if let Some(b_as_idx_plus_1) = b_as_idx_plus_1 {
                let rho_diff_curve = rho_diff_curve
                    .get(b_as_idx_plus_1 - 1)
                    .unwrap_or_else(|| &0.);

                // CALCULATE Kb Sb for a bucket
                let buck_kb_sb = bucket_kb_sb_single_type(
                    bucket_df,
                    rho_same_curve,
                    *rho_diff_curve,
                    scenario_fn,
                    columns,
                    None,
                    special_bucket,
                );
                let mut res = arc_mtx.lock().unwrap();
                res[b_as_idx_plus_1 - 1] = buck_kb_sb;
            }
        });

    let reskbs_sbs: PolarsResult<Vec<(String, (f64, f64))>> = Arc::try_unwrap(arc_mtx)
        .map_err(|_| PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_| PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .into_iter()
        .collect();

    let buckets_kbs_sbs = reskbs_sbs?;
    let (_buckets, (kbs, sbs)): (Vec<String>, (Vec<f64>, Vec<f64>)) =
        buckets_kbs_sbs.into_iter().unzip();

    //let (buckets_kbs, sbs): (Vec<(String, f64)>, Vec<f64>) = buckets_kbs_sbs.into_iter().unzip();
    //let (_buckets, kbs): (Vec<String>, Vec<f64>) = buckets_kbs.into_iter().unzip();

    Ok(kbs.into_iter().zip(sbs.into_iter()).collect())
}

/// Girr Delta and Eq, CSR and Commodity Vega share common approach.
/// They have tenors and no RFT (in case of GIRR Infl and XCCY become columns)
pub(crate) fn bucket_kb_sb_single_type<F>(
    bucket_df: &DataFrame,
    rho_same_curve: &Array2<f64>,
    rho_diff_curve: f64,
    scenario_fn: F,
    columns: &[&'static str],
    girr: Option<(f64, f64)>,
    special_bucket: Option<&'static str>,
) -> PolarsResult<(String, (f64, f64))>
where
    F: Fn(f64) -> f64 + Sync + Send + Copy,
{
    let bucket = unsafe {
        bucket_df["b"]
            .utf8()?
            .get_unchecked(0)
            .unwrap_or_else(|| "Default")
    }
    .to_string();
    let mut sb = 0.; //this is Sb
    let mut var_covar_sum = 0.; // this is pre Kb(var-covar sum) for a single tenor
    let mut cross_tenor = 0.;
    // TODO here and for every rho, for vega we need to make sure rho = min(rho_delta*rho_opt_mat; 1)
    // as per 21.94. This can be achieved by passing an additional Vega flag.
    // However, does this make sence? Every rho in the text is less than or equal to(in case of opt mat same tenor) 1
    // Hence, can we ever have rho over 1? Doesn't seem so.
    let case1 = scenario_fn(rho_diff_curve); //Same tenor, diff curve
    let yield_df = bucket_df
        .select(columns)?
        .fill_null(FillNullStrategy::Zero)?;
    let all_yield_arr = yield_df.to_ndarray::<Float64Type>(IndexOrder::Fortran)?;

    // EQ Vega, take care of special bucket
    match special_bucket {
        Some(x) if x == bucket.as_str() => {
            let abs_sum = all_yield_arr.iter().map(|x| x.abs()).sum::<f64>();
            return Ok((x.to_string(), (abs_sum, abs_sum)));
        }
        _ => (),
    };

    // If this is a GIRR calculation, then compute XCCY and Inflation
    if let Some((rho_infl, rho_xccy)) = girr {
        let xccy: f64 = bucket_df["XCCY"].sum().unwrap_or_else(|| 0.);
        // 21.8.2.b
        let infl: f64 = bucket_df["Inflation"].sum().unwrap_or_else(|| 0.);
        sb = sb + xccy + infl;

        var_covar_sum = var_covar_sum + xccy.powi(2) + infl.powi(2);
        let case2 = scenario_fn(rho_infl); // Yield (any tenor) vs Infl
        let case3 = scenario_fn(rho_xccy); // Yield (any tenor) vs XCCY
        var_covar_sum += case2 * 2f64 * infl * all_yield_arr.sum();
        var_covar_sum += case3 * 2f64 * xccy * all_yield_arr.sum();
    }

    for (i, c1) in columns.iter().enumerate() {
        //let _i = 1;
        let (sum, var_covar) = var_covar_sum_single(&yield_df[*c1], case1)?;
        sb += sum;
        var_covar_sum += var_covar;

        if columns[(i + 1)..].is_empty() {
            continue;
        };
        let mut current_yield_arr = yield_df[*c1].f64()?.to_ndarray()?.to_owned();
        let next_yields_arr = all_yield_arr.slice(s![.., (i + 1)..]);

        let mut tenor_rho_uninit = Array2::<f64>::uninit(next_yields_arr.raw_dim());
        tenor_rho_uninit
            .axis_iter_mut(Axis(1))
            .enumerate()
            .for_each(|(col, mut x)| {
                // i is the index of tenor col (eg 025Y is 0, etc)
                // tenor_rho is rho matrix against !next! tenors
                // Hence we look up i+1. If i+1 is non existent we would've exited by now
                // col is the col of the !next! tenor
                let cross_tenor_rho = unsafe { rho_same_curve.uget((i, i + 1 + col)) };
                x.fill(MU::new(*cross_tenor_rho));
            });

        let mut tenor_rho: Array2<f64>;
        unsafe { tenor_rho = tenor_rho_uninit.assume_init() };

        let mut diff_curve_diff_tenors = rho_diff_curve * tenor_rho.to_owned();
        diff_curve_diff_tenors.par_mapv_inplace(scenario_fn);
        tenor_rho.par_mapv_inplace(scenario_fn);
        let next_yields_weighted = diff_curve_diff_tenors * next_yields_arr;
        let next_yields_weighted_sum = next_yields_weighted.sum();
        let next_yields_weighted_sum_cols = next_yields_weighted.sum_axis(Axis(1));
        let next_yields_weighted_same_curve = tenor_rho * next_yields_arr;
        //let next_yields_weighted_same_curve_sum = next_yields_weighted.sum();
        let next_yields_weighted_same_curve_sum_cols =
            next_yields_weighted_same_curve.sum_axis(Axis(1));

        current_yield_arr
            .indexed_iter_mut()
            .par_bridge()
            .for_each(|(j, v)| {
                if *v != 0f64 {
                    let cross_sum = unsafe {
                        next_yields_weighted_sum - next_yields_weighted_sum_cols.uget(j)
                            + next_yields_weighted_same_curve_sum_cols.uget(j)
                    };

                    *v = 2f64 * (*v) * cross_sum;
                }
            });

        cross_tenor += current_yield_arr.sum();
    }
    let kb = (var_covar_sum + cross_tenor).max(0.).sqrt();
    Ok((bucket, (kb, sb)))
}
