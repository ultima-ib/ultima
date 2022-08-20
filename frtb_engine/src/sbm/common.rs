use base_engine::prelude::*;

use log::warn;
use ndarray::{Array2, Array1, Zip, ArrayView1, Array, Order, s, Axis};
use polars::{prelude::*, export::num::Signed};
use rayon::{iter::{ParallelBridge, ParallelIterator, IntoParallelRefMutIterator}, prelude::IntoParallelRefIterator};
use std::{sync::Mutex};
use std::mem::MaybeUninit as MU;

/// Sum of all delta sensis, from spot to 30Y tenor
/// In practice should be used only with filter on RiskClass
/// as combining FX and IR sensis is meaningless
pub fn total_delta_sens() -> Expr {
    // When adding Exprs NULLs have to be filled
    // Otherwise returns NULL
    ( col("SensitivitySpot").fill_null(0.)
    +col("Sensitivity_025Y").fill_null(0.)
    +col("Sensitivity_05Y").fill_null(0.)
    +col("Sensitivity_1Y").fill_null(0.)
    +col("Sensitivity_2Y").fill_null(0.) 
    +col("Sensitivity_3Y").fill_null(0.)
    +col("Sensitivity_5Y").fill_null(0.)
    +col("Sensitivity_10Y").fill_null(0.)
    +col("Sensitivity_15Y").fill_null(0.)
    +col("Sensitivity_20Y").fill_null(0.)
    +col("Sensitivity_30Y").fill_null(0.) )
    // To be removed after this fixed is published on crates
    //https://github.com/pola-rs/polars/issues/4326
    .cast(DataType::Float64)
}

pub fn total_vega_curv_sens() -> Expr {
    // When adding Exprs NULLs have to be filled
    // Otherwise returns NULL
    ( 
    col("Sensitivity_05Y").fill_null(0.)
    +col("Sensitivity_1Y").fill_null(0.)
    +col("Sensitivity_3Y").fill_null(0.)
    +col("Sensitivity_5Y").fill_null(0.)
    +col("Sensitivity_10Y").fill_null(0.) )
    // To be removed after this fixed is published on crates
    //https://github.com/pola-rs/polars/issues/4326
    .cast(DataType::Float64)
}

pub(crate) fn total_sens_curv_weighted() -> Expr {
    total_delta_sens()*col("CurvatureRiskWeight")
}
pub(crate) fn cvr_up() -> Expr {
    lit::<f64>(0.) - ( col("PnL_Up") - total_sens_curv_weighted() )
}

pub(crate) fn cvr_down() -> Expr {
    lit::<f64>(0.) - ( col("PnL_Down") + total_sens_curv_weighted() )
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
/// TODO remove cast
/// https://github.com/pola-rs/polars/issues/4326
pub(crate) fn spot_sens_curv_weighted() -> Expr {
    col("SensitivitySpot").fill_null(0.).cast(DataType::Float64)*col("CurvatureRiskWeight")
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
pub(crate) fn cvr_up_spot() -> Expr {
    lit::<f64>(0.) - ( col("PnL_Up") - spot_sens_curv_weighted() )
}
/// This is for Risk Classes where only Spot Delta constitutes risk, ie FX, Eq
pub(crate) fn cvr_down_spot() -> Expr {
    lit::<f64>(0.) - ( col("PnL_Down") + spot_sens_curv_weighted() )
}

pub(crate) fn rc_cvr(rc: &'static str, dir: CVR)->Expr{
    let cvr = match dir {
        CVR::Up  => cvr_up(),
        CVR::Down => cvr_down(),
    };
    rc_sens(rc, cvr)
}
pub(crate) fn rc_cvr_spot(rc: &'static str, dir: CVR)->Expr{
    let cvr = match dir {
        CVR::Up  => cvr_up_spot(),
        CVR::Down => cvr_down_spot(),
    };
    rc_sens(rc, cvr)
}

pub(crate) enum CVR {
    Up,
    Down
}

pub(crate) fn curv_delta_total(rc: &'static str) -> Expr {
    curv_delta(rc, total_delta_sens())
}

pub(crate) fn curv_delta_spot(rc: &'static str) -> Expr {
    curv_delta(rc, col("SensitivitySpot"))
}

/// Filtering total delta on risk class and PnL Up or PnL Down is not null
/// giving us Curvature Delta
fn curv_delta(rc: &'static str, risk: Expr) -> Expr {

    apply_multiple(  move |columns| {
         
        //RiskClass
        let mask = columns[0]
            .utf8()?
            .equal(rc);
        
        let mask1 = columns[1]
            .f64()?
            .is_not_null();

        let mask2 = columns[2]
            .f64()?
            .is_not_null();
        
        let pnl_up_or_down_is_not_null = mask1|mask2;
        
        let risk_filtered = columns[3]
            .f64()?
            .set(&!(mask&pnl_up_or_down_is_not_null), None)?;

        Ok(risk_filtered.into_series())
    }, 
        &[col("RiskClass"), col("PnL_Up"), col("PnL_Down"), risk], 
        GetOutput::from_type(DataType::Float64))
}

/// Filtering risk on rcat and risk class
pub fn rc_rcat_sens(rcat: &'static str, rc: &'static str, risk: Expr) -> Expr {

    apply_multiple(  move |columns| {
         
        //RiskClass
        let mask = columns[0]
            .utf8()?
            .equal(rc);
        //RiskCategory
        let mask1 = columns[1]
            .utf8()?
            .equal(rcat);
        
        let risk_filtered = columns[2]
            .f64()?
            .set(&!(mask&mask1), None)?;

        Ok(risk_filtered.into_series())
    }, 
        &[col("RiskClass"), col("RiskCategory"), risk], 
        GetOutput::from_type(DataType::Float64))
}

/// Filtering risk on risk class
pub fn rc_sens(rc: &'static str, risk: Expr) -> Expr {

    apply_multiple(  move |columns| {
         
        //RiskClass
        let mask = columns[0]
            .utf8()?
            .equal(rc);
        
        let risk_filtered = columns[1]
            .f64()?
            .set(&!mask, None)?;

        Ok(risk_filtered.into_series())
    }, 
        &[col("RiskClass"), risk], 
        GetOutput::from_type(DataType::Float64))
}

/// Helper function to derive weighted delta,
/// per tenor, per risk class, per risk Category
/// TODO allow SensWeights OR SensWeights depending on Reporing.
pub fn rc_tenor_weighted_sens(rcat: &'static str, rc: &'static str, delta_tenor: &str, weights_col: &str, weight_idx: i64) -> Expr {

    apply_multiple(  move |columns| {
         
        //RiskClass
        let mask = columns[0]
            .utf8()?
            .equal(rc);
        //RiskCategory
        let mask1 = columns[3]
            .utf8()?
            .equal(rcat);
        
        let delta = columns[1]
            .f64()?
            .set(&!(mask&mask1), None)?;
        
        let x = delta.multiply(&columns[2])?;
        Ok(x)
    }, 
        &[col("RiskClass"), col(delta_tenor), col(weights_col).arr().get(weight_idx), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
}

///makes sence at RiskClass-Bucket view
pub fn sens_weights(_: &OCP) -> Expr {
    col("SensWeights")
}

pub(crate) fn across_bucket_agg<I: IntoIterator<Item = f64>>(kbs: I, sbs: I, gamma: &Array2<f64>, 
    res_len: usize, sbm_type: SBMChargeType) 
-> Result<Series>
 {
    let kbs_arr = Array1::from_iter(kbs);        
    let sbs_arr = Array1::from_iter(sbs);

    //21.4.5 sum{ sum {gamma*s_b*s_c} }
    let a = sbs_arr.dot(gamma);
    let b = a.dot(&sbs_arr);

    //21.4.5 sum{Kb^2}
    let c = kbs_arr.dot(&kbs_arr);

    let sum = c+b;

    let res = match sbm_type {

        SBMChargeType::DeltaVega => {if sum < 0. {
        //21.4.5.b
        let sbs_alt = alt_sbs(sbs_arr.view(), kbs_arr.view());
        //now recalculate capital charge with alternative sb
        //21.4.5 sum{ sum {gamma*s_b*s_c} }
        let a = sbs_alt.dot(gamma);
        let b = a.dot(&sbs_alt);
        //21.4.5 sum{K-b^2}
        let c = kbs_arr.dot(&kbs_arr);
        let sum = c+b;
        sum.sqrt()
        } else {
            sum.sqrt()
        }},

        SBMChargeType::Curvature => f64::max(sum, 0.).sqrt(),
    };

    // The function is supposed to return a series of same len as the input, hence we broadcast the result
    let res_arr = Array::from_elem(res_len, res);
    // if option panics on .unwrap() implement match and use .iter() and then Series from iter
    Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
}

pub(crate) enum SBMChargeType{
    DeltaVega,
    Curvature
}

pub(crate) fn alt_sbs(sbs_arr: ArrayView1<f64>, kbs_arr: ArrayView1<f64>) -> Array1<f64>{
   //21.4.5.b
   let mut sbs_alt = Array1::<f64>::zeros(kbs_arr.raw_dim());
   Zip::from(&mut sbs_alt)
       .and(sbs_arr)
       .and(kbs_arr)
       .par_for_each(|alt, &sb, &kb|{
           let _min = sb.min(kb);
           *alt = _min.max(-kb);
   });
   sbs_alt
}

/// Common function used for CSR, Commodity and EquityVega
/// 
/// We compare name_col(RF), and where not equal we multiply rho_tenor by rho_diff_rf_bucket[bucket_id]
/// 
/// Equity case is special, as we only need to compare name(RiskFactor) and not basis(RiskFactorType or Location).
/// Hence basis_col(RFT/Loc) arg is optional, and if provided then rho_diff_rft is expected as well.
/// 
/// Computes kb and sb efficiently via uninit
#[deprecated(note = "Initialises massive matrix which is not practical with large portfolios")]
pub(crate) fn bucket_kb_sb<F>(df: LazyFrame, bucket_id: usize, special_bucket: Option<usize>, 
    rho_tenor: &Array2<f64>, name_col: &str, rho_diff_rf_bucket: &[f64], 
    basis_col: Option<&str>, rho_diff_rft: Option<f64>,  scenario_fn: F, tenor_cols:Vec<&str>, ) 
-> Result<(f64, f64)> 
where F: Fn(f64) -> f64 + Sync + Send,
{
    let bucket_df = df
            .filter(col("b").eq(lit(bucket_id.to_string())))
            .collect()?;
        
    let n_curves = bucket_df.height();
    if bucket_df.height() == 0 { return Ok((0.,0.)) };

    let n_tenors = tenor_cols.len();
    let mut ws_arr = bucket_df
                .select(tenor_cols)?
                .to_ndarray::<Float64Type>()?;
    // 21.56 
    match special_bucket {
        Some(x) if x==bucket_id => {
            ws_arr.par_iter_mut().for_each(|x|*x=x.abs());
            return Ok((ws_arr.sum(),ws_arr.sum()))
        },
        _ => (),
    };
    // Array used for name(RF) comparison
    let name_arr = bucket_df[name_col].utf8()?;
    // Array used for (optional) RFT/Loc comparison
    let empty_arr = Utf8Chunked::default();
    let curve_type_arr = match basis_col{
            Some(basis_col_name) => bucket_df[basis_col_name].utf8()?,
            _ => &empty_arr,
    };

    let rho_name_bucket = rho_diff_rf_bucket.get(bucket_id-1)
        .map(|x|*x)
        .unwrap_or_else(||0.);

    let mut arr = Array2::<f64>::uninit((n_curves*n_tenors, n_curves*n_tenors));
    arr
    .exact_chunks_mut((n_tenors, n_tenors))
    .into_iter()
    .enumerate()
    .par_bridge()
    .for_each(|(i, chunk)|{
        let row_id = i/n_curves; //eg 27usize/10usize = 2usize
        let col_id = i%n_curves; //eg 27usize % 10usize = 7usize
        
        let name_rho = if 
            unsafe{ name_arr.get_unchecked(row_id) } 
            == unsafe{ name_arr.get_unchecked(col_id) }{
                1.
            } else {
                rho_name_bucket
            };

        let basis_rho = match basis_col{
            // if basis_col was provided, then we compare curve_type_arr(RFT array)
            Some(_) =>{ if
                unsafe{ curve_type_arr.get_unchecked(row_id) } 
                == unsafe{ curve_type_arr.get_unchecked(col_id) } {
                    1.
                } else {
                    rho_diff_rft.expect("basis_col was provided, hence expect rho_diff_rft")
                }
            },
            _ => 1.
        };

        (rho_tenor*name_rho*basis_rho).move_into_uninit(chunk);
    });
    let mut rho: Array2<f64>;
    unsafe {
        rho = arr.assume_init();
    }
    println!("Rho in bucket {bucket_id} initialised");
    rho.par_mapv_inplace(|el| {scenario_fn(el)});
    // Stretch out the array into a single vector
    let arr_shaped = ws_arr
            .to_shape((ws_arr.len(), Order::RowMajor) )
            .map_err(|_| PolarsError::ShapeMisMatch("Could not reshape csr arr".into()) )?;
    //21.4.4
    let a = arr_shaped.dot(&rho);
    //21.4.4
    let kb = a.dot(&arr_shaped)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = arr_shaped.sum();
    
    Ok((kb,sb))
}

/// Column "b" expected. Value in col "b" is expected to be parsable into usize
/// 
/// Internally calls [bucket_kb_sb].
/// 
/// Common way of calculating Kbs and Sbs for all buckets
/// Used for CSR, Commodity, Equity Vega
/// 
/// df must contain column "b", which represent a bucket
#[deprecated(note = "wrapper around bucket_kb_sb which is being deprecated")]
pub(crate) fn all_kbs_sbs<F>(df: DataFrame, tenor_cols:Vec<&str>, n_buckets: usize, 
rho_tenor:&Array2<f64>, name_col: &str, bucket_rho_diff_rf: &[f64], basis_col: Option<&str>, rho_base_diff_rft_or_loc: Option<f64>, 
scenario_fn: F, special_bucket: Option<usize>) 
-> Result<Vec<(f64, f64)>>
where F: Fn(f64) -> f64 + Sync + Send + Copy{

    let mut reskbs_sbs: Vec<Result<(f64, f64)>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets{reskbs_sbs.push(Ok((0., 0.)))};

    let arc_mtx = Arc::new(Mutex::new(reskbs_sbs));
    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    // 
    df["b"]// We know column "b" exists, so will never panic here
    .utf8()?
    .unique()?
    .par_iter()
    .for_each(|b|{
        match b {
            Some(_b) => {
                let mut b_as_idx_plus_1 = _b.parse::<usize>()
                .unwrap_or_else(|_|{
                    warn!("{_b} cannot be parsed into an int representing commodity the bucket. Default to 1.");
                    1usize});
                if b_as_idx_plus_1 > n_buckets {
                    warn!("{_b} is larger than the max bucket for this risk class. Default to 1.");
                    b_as_idx_plus_1 = 1usize;
                }
                // CALCULATE Kb Sb for a bucket
                let a = bucket_kb_sb(df.clone().lazy(), b_as_idx_plus_1, special_bucket,
                    rho_tenor, name_col, bucket_rho_diff_rf, basis_col,
                     rho_base_diff_rft_or_loc, scenario_fn,
                    tenor_cols.clone() );
                let mut res = arc_mtx.lock().unwrap();
                res[b_as_idx_plus_1-1] = a;
            },
            _=>()
        }
    });
    let reskbs_sbs: Result<Vec<(f64, f64)>> = Arc::try_unwrap(arc_mtx)
        .map_err(|_|PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_|PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .into_iter()
        .collect();
    reskbs_sbs
}

/// 21.93
pub fn option_maturity_rho() -> Array2<f64> {
    let mut option_maturity_rho = Array2::<f64>::zeros((5, 5));
    let tenors = [ 0.5, 1., 3., 5., 10.];

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
    f64::exp(-alpha*f64::abs(m1-m2)/f64::min(m1,m2))
}

///21.5.3.b
pub(crate) fn phi(sbs: &Vec<f64>) -> Array2<f64> {
    let mut arr = Array2::ones((sbs.len(), sbs.len()));
    let mut tmp: Vec<usize> = Vec::with_capacity(sbs.len()); // To keep track on negative Sbs
    for (i, v) in sbs.iter().enumerate() {
        if *v<0.{
            for t in &tmp {
                unsafe{*arr.uget_mut((i,*t)) = 0.};
                unsafe{*arr.uget_mut((*t,i)) = 0.};
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
/// Used for Eq, Girr curvature.
pub(crate) fn kb_plus_minus_simple(cvr_up_or_down: &Series) -> Result<Vec<f64>>{
    Ok(
    cvr_up_or_down
    .f64()?
    .into_iter()
    .map(|cvr|
        // No need to ^2 and SQRT since below is just one positive(or 0) number
        f64::max(cvr.unwrap_or_else(||0.), 0.)
    )
    .collect()
    )
}

/// calculates kb plus, kb minus, sb, kb per bucket simultaniously 
/// * `df` expected to have columns "b", "cvr_up", "cvr_down"
pub(crate) fn curvature_kb_plus_minus(
    df: DataFrame,
    n_buckets: usize, 
    bucket_rho: &[f64],
    special_bucket: Option<usize>
    ) 
    -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    
        let mut res_kb_cvr: Vec< (Result<(f64, f64)>, Result<(f64, f64)>) > = Vec::with_capacity(n_buckets);
        //let mut res_kbminus_cvrdown: Vec<Result<(f64, f64)>> = Vec::with_capacity(n_buckets);
        for _ in 0..n_buckets{res_kb_cvr.push(( Ok((0., 0.)), Ok((0., 0.)) )) };
    
        let arc_mtx_kbpm_cvr = Arc::new(Mutex::new(res_kb_cvr));

        // Do not iterate over each bukcet. Instead, only iterate over unique buckets
        df.partition_by(["b"])?
        .par_iter()
        .for_each(|bucket_df|{
            // Ok to go unsafe here becaause we validate length in [equity_delta_charge_distributor]
            let b_as_idx_plus_1 = unsafe{ bucket_df["b"].get_unchecked(0)};
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st)=>{ st.parse::<usize>().ok().and_then(|b_id|{if b_id<n_buckets{Some(b_id)}else{None}}).unwrap_or_else(||1)}
            ,   _=>1};

            let is_special_bucket = match special_bucket {
                Some(b) if b == b_as_idx_plus_1 => true,
                _=>false,
            };
            let rho = bucket_rho[b_as_idx_plus_1-1];
            // CALCULATE Kb Sb for a bucket
            let buck_kb_plus_cvr_up_sum = kb_plus_minus(&bucket_df["cvr_up"], rho, is_special_bucket);
            let buck_kb_minus_cvr_down_sum = kb_plus_minus(&bucket_df["cvr_down"], rho, is_special_bucket);
            arc_mtx_kbpm_cvr.lock().unwrap()[b_as_idx_plus_1-1] = (buck_kb_plus_cvr_up_sum, buck_kb_minus_cvr_down_sum);
            }
        );
        //Result<Vec<(f64, f64)>>
        let (res_kbp_cvrup, res_kbm_cvrdown): (Vec<Result<(f64, f64)>>, Vec<Result<(f64, f64)>>)  = Arc::try_unwrap(arc_mtx_kbpm_cvr)
            .map_err(|_|PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
            .into_inner()
            .map_err(|_|PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
            .into_iter()
            .unzip();

        let res_kbp_cvrup: Result<Vec<(f64, f64)>> = res_kbp_cvrup.into_iter().collect();
        let res_kbm_cvrdown: Result<Vec<(f64, f64)>> = res_kbm_cvrdown.into_iter().collect();
        Ok( (res_kbp_cvrup?,res_kbm_cvrdown?) )
    }


/// ( Curvature Kb Plus/Minus , CVR_UP_DOWN Sum )
/// * `special_bucket` indicates if CVR Up/Down to be calculated accordingly to 21.79
pub(crate) fn kb_plus_minus(cvr_up_or_down: &Series, rho: f64, special_bucket: bool) -> Result<(f64, f64)>{
    // If special bucket, we simply sum positive values for Kb puls/minus
    if special_bucket {
        let sum_positive: f64 = cvr_up_or_down.f64()?.into_no_null_iter()
        .filter(|x|{
            x.is_positive()
        }).sum();
        let sum = cvr_up_or_down.f64()?.sum().unwrap_or_default();
        return Ok((sum_positive, sum))
    }
    //First, calculate as if normal var covar sum
    let (sum, full) = var_covar_sum_single(cvr_up_or_down, rho)?;
    //Then, subtract squares where max(CVR,0)^2 would be 0 (ie squares of negative numbers)
    //And Correlations of negative numbers. For this first get srs of negative items
    let neg_srs = cvr_up_or_down.f64()?.into_iter()
        .filter(|o|{
            if let Some(x) = o {
                x.is_negative()
            }  else {false}
        })
        .collect::<Series>();
    let (_, neg) = var_covar_sum_single(&neg_srs, rho)?;

    Ok( ( (full - neg).max(0.).sqrt(), sum) )
}

/// Computes Kb, Sb from kb_plus, kb_minus, sum_cvr_up, sum_cvr_down.
/// TODO take cre of special bucket
pub(crate) fn kbs_sbs_curvature<I>(kb_plus: Vec<f64>,kb_minus: Vec<f64>, cvr_up: I, cvr_down: I) -> Result<(Vec<f64>, Vec<f64>)> 
where I: Iterator<Item = Option<f64>>
{
    let kbs_sbs: Vec<(f64, f64)> = kb_plus.into_iter()
        .zip(kb_minus.into_iter())
        .zip(cvr_up)
        .zip(cvr_down)
        .map(| (((kb_p, kb_m), cv_up), cv_down)|
            if kb_p>kb_m{
                (kb_p, cv_up.unwrap_or_else(||0.))
            } else if kb_m>kb_p {
                (kb_m, cv_down.unwrap_or_else(||0.))
            } else { // 21.5.3.a.iii
                if cv_up>cv_down{
                    (kb_p, cv_up.unwrap_or_else(||0.))
                } else {
                    (kb_m, cv_down.unwrap_or_else(||0.))
                }
            }
        )
        .collect::<Vec<(f64, f64)>>();
    let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
    Ok((kbs, sbs))
}

/// Equity and CSRnonSec and CSR sec CTP share common approach
/// They have limited number of buckets.
/// They have 2 variants for RFT, and many different names
/// The difference between them is only in number of tenors 
/// *df is expected to have "b" column representing bucket
/// TODO use bucket_rho_diff_rf.len() instead of n_buckets
pub(crate) fn all_kbs_sbs_two_types_w_tenors<F>(df: DataFrame, n_buckets: usize, bucket_rho_diff_rf: &[f64], 
    rho_base_diff_rft_or_loc: f64, 
    scenario_fn: F, special_bucket: Option<usize>,
    cols_by_tenor: &[(&str, &str)],
    dtenor: Option<f64>,
    ) 
    -> Result<Vec<(f64, f64)>>
    where F: Fn(f64) -> f64 + Sync + Send + Copy{
        
        let mut reskbs_sbs: Vec<Result<(f64, f64)>> = Vec::with_capacity(n_buckets);
        for _ in 0..n_buckets{reskbs_sbs.push(Ok((0., 0.)))};
    
        let arc_mtx = Arc::new(Mutex::new(reskbs_sbs));
        // Do not iterate over each bukcet. Instead, only iterate over unique buckets
        // 
        df.partition_by(["b"])?
        .par_iter()
        .for_each(|bucket_df|{
            // Ok to go unsafe here becaause we validate length in [equity_delta_charge_distributor]
            let b_as_idx_plus_1 = unsafe{ bucket_df["b"].get_unchecked(0)};
            // validating also bucket is not greater than max index of bucket_rho_diff_rf
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st)=>{ st.parse::<usize>().ok().and_then(|b_id|{if b_id<n_buckets{Some(b_id)}else{None}}).unwrap_or_else(||1)}
            ,   _=>1};

            // CALCULATE Kb Sb for a bucket
            let buck_kb_sb = bucket_kb_sb_two_types_w_tenors(bucket_df, b_as_idx_plus_1, special_bucket,
                bucket_rho_diff_rf,  rho_base_diff_rft_or_loc, scenario_fn, cols_by_tenor, dtenor );
            let mut res = arc_mtx.lock().unwrap();
            res[b_as_idx_plus_1-1] = buck_kb_sb;
            }
        );
        let reskbs_sbs: Result<Vec<(f64, f64)>> = Arc::try_unwrap(arc_mtx)
            .map_err(|_|PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
            .into_inner()
            .map_err(|_|PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
            .into_iter()
            .collect();
        reskbs_sbs
    }

/// This function assumes two RFTs
/// * `df` - A "pivoted" Dataframe. Rows are names(RFs), columns are 2(for each RFT) x ntenors. Expecting no NULLs
/// * `tenor_cols` , all columns are expected to .f64(), otherwise we will panic
pub(crate) fn bucket_kb_sb_two_types_w_tenors<F>(df: &DataFrame, bucket_id: usize, special_bucket: Option<usize>,
    rho_diff_rf_bucket: &[f64], rho_diff_rft: f64, scenario_fn: F, cols_by_tenor: &[(&str, &str)],
dtenor: Option<f64> ) 
    -> Result<(f64, f64)> 
    where F: Fn(f64) -> f64 + Sync + Send,
    { 
      
        //let flt = flatten6(cols_by_tenor);
        // 21.56 First, take care of the special bucket
        match special_bucket {
            Some(x) if x==bucket_id => {
                let mut abs_sum = 0.;
                for (c1, c2)in cols_by_tenor.iter() {
                    abs_sum += df[*c1]
                    .f64()?
                    .into_no_null_iter()
                    .map(|x| x.abs())
                    .sum::<f64>() 
                    +
                    df[*c2]
                    .f64()?
                    .into_no_null_iter()
                    .map(|x| x.abs())
                    .sum::<f64>() ;
                }; 
                return Ok((abs_sum,abs_sum))
            },
            _ => (),
        };
        
        let rho_name_bucket = unsafe{rho_diff_rf_bucket.get_unchecked(bucket_id-1)};
        let rho_case1 =  scenario_fn(*rho_name_bucket);//Diff name, same type
        let rho_case2 =  scenario_fn(rho_diff_rft);//Diff type, same name
        let rho_case3 =  scenario_fn(rho_name_bucket*rho_diff_rft);//Diff name, diff type

        let mut sb = 0.; //this is Sb
        let mut var_covar_sum = 0.; // this is pre Kb(var-covar sum) for a single tenor
        let mut cross_tenor = 0.;
        
        for (t, (c1, c2)) in cols_by_tenor.iter().enumerate() {
            //println!("{t}");
            let (pre_kb, pre_sb) = var_covar_sum_fn(&df[*c1], &df[*c2], rho_case1, rho_case2, rho_case3);
            sb += pre_sb;
            var_covar_sum += pre_kb;
            // Now, if there are any other tenors, we need to account for them
            if cols_by_tenor[(t+1)..].is_empty(){continue};
            // First, check dtenor was provided
            if let Some(dt) = dtenor {
                let rho_case4 =  scenario_fn(dt);
                let rho_case5 =  scenario_fn(dt*rho_diff_rft);
                let rho_case6 =  scenario_fn(dt*rho_name_bucket);
                let rho_case7 =  scenario_fn(dt*rho_diff_rft*rho_name_bucket);

                
                let mut arr_tenor = df.select([*c1, *c2])?
                    .to_ndarray::<Float64Type>()?; // Nulls must've been filled
                let dim = arr_tenor.raw_dim();
                let mut next_tenors_sum = Array2::<f64>::zeros(dim);
                for (c3, c4) in cols_by_tenor[(t+1)..].iter(){
                    let next_tenor = df.select([*c3, *c4])?
                    .to_ndarray::<Float64Type>()?; // Nulls must've been filled
                    next_tenors_sum = next_tenors_sum + next_tenor;
                }
                arr_tenor.indexed_iter_mut()
                .par_bridge()
                .for_each(|((i, j), v)|{
                    let anti_j: usize = if j==0{1}else{0}; // j is either 0 or 1
                    let mut uninit_rho = Array2::<f64>::uninit(dim);
                    let  (mut diff_name_same_type1, mut diff_name_diff_type1, 
                    mut same_name_same_type, mut same_name_diff_type,
                    mut diff_name_same_type2, mut diff_name_diff_type2) =
                    uninit_rho.multi_slice_mut((
                            s![0..i,j],    s![0..i,anti_j],
                            s![i, j],      s![i, anti_j],
                            s![(i+1)..,j], s![(i+1)..,anti_j]
                            ));
                    
                    diff_name_same_type1.fill(MU::new(rho_case6));
                    diff_name_diff_type1.fill(MU::new(rho_case7));
                    same_name_same_type.fill(MU::new(rho_case4));
                    same_name_diff_type.fill(MU::new(rho_case5));
                    diff_name_same_type2.fill(MU::new(rho_case6));
                    diff_name_diff_type2.fill(MU::new(rho_case7));

                    let rho: Array2<f64>;
                    unsafe {
                        rho = uninit_rho.assume_init();
                    }
                    let a = 2.*(rho*next_tenors_sum.view()*(*v)).sum();
                    *v = a;
                });
            
                cross_tenor += arr_tenor.sum();        
            }
        };

        let kb = (var_covar_sum+cross_tenor).max(0.).sqrt();
        Ok((kb,sb)) 
    }

/// Calculates Var-Covar Matrix in O(N) for !two distinct risk types!
/// * `srs1` and `srs2` are expected to be ".f64()"
/// * `rho_case1` Diff name, same type (diff name, non-nn)
/// * `rho_case2` Same name, diff type (same name, non-nn)
/// * `rho_case3` Diff name, diff type (_, nn)
pub(crate) fn var_covar_sum_fn(srs1: &Series, srs2: &Series, rho_case1: f64, rho_case2: f64, rho_case3: f64) -> (f64, f64) {

    //let (spot_sum, spot_f1) = var_covar_sum_single(srs1, rho_case1);
    //let (repo_sum, repo_f1) = var_covar_sum_single(srs2, rho_case1);

    let mut spot_sum = 0.;
    let mut spot_sum_of_sq = 0.;
 
    let mut repo_sum = 0.;
    let mut repo_sum_of_sq = 0.;
 
    let mut spot_times_repo_sum = 0.;

    srs1.f64().unwrap().into_iter()
    .zip(srs2.f64().unwrap().into_iter())
    .for_each(|(x,y)|{
        let x = x.unwrap_or_default();
        let y = y.unwrap_or_default();
        spot_sum+=x;
        spot_sum_of_sq+=x.powi(2);
        repo_sum+=y;
        repo_sum_of_sq+=y.powi(2);
        spot_times_repo_sum = spot_times_repo_sum + x*y;

    });
    let pre_sb = repo_sum + spot_sum;

    let formula1 = spot_sum_of_sq + repo_sum_of_sq + rho_case1*(spot_sum.powi(2) - spot_sum_of_sq + repo_sum.powi(2) - repo_sum_of_sq );
    //let formula1 = spot_f1 + repo_f1;
    
    let formula2_pt1 = spot_sum*repo_sum*rho_case3;
    // .sum() returns None if array is empty or all NULLs
    //let spot_times_repo_sum = (srs1*srs2).f64().unwrap().sum().unwrap_or_default();//.unwrap_or_else(||0.);

    let formula2_pt2 = rho_case2*spot_times_repo_sum;
    let formula2_pt3 = - rho_case3*spot_times_repo_sum; 
    let formula2 = formula2_pt1+formula2_pt2+formula2_pt3;   
    
    let pre_kb = formula1+2f64*formula2;
    
    (pre_kb, pre_sb)
} 

/// Rho represents rho between risk factors where name/rf is different
/// 
/// Returns: (sum(for Sb), formula1(Kb) )
pub(crate) fn var_covar_sum_single(srs: &Series, rho: f64)->Result<(f64, f64)>{
    let mut sum_of_sq=0.;
    let mut sum=0.;
    srs.f64()?
    .into_no_null_iter()
    .for_each(|x|{
        sum_of_sq += x.powi(2);
        sum += x;
    });
    
    let f1 = 
    sum_of_sq
    - rho*sum_of_sq
    + rho*sum.powi(2);

    Ok( (sum, f1) )
}

pub(crate) fn all_kbs_sbs_single_type<F>(
    df: DataFrame, 
    n_buckets: usize, 
    rho_same_curve: &Array2<f64>, 
    rho_diff_curve: &[f64],
    scenario_fn: F,
    columns: &[&'static str],
    special_bucket: Option<&'static str>
    ) 
    -> Result<Vec<(f64, f64)>>
    where F: Fn(f64) -> f64 + Sync + Send + Copy{
    
        let mut reskbs_sbs: Vec<Result<((String, f64), f64)> > = Vec::with_capacity(n_buckets);
        for _ in 0..n_buckets{reskbs_sbs.push( Ok( (("".to_string(), 0.), 0.) ))};
    
        let arc_mtx = Arc::new(Mutex::new(reskbs_sbs));
        // Do not iterate over each bukcet. Instead, only iterate over unique buckets
        // 
        df.partition_by(["b"])?
        .par_iter()
        .for_each(|bucket_df|{
            // Ok to go unsafe here becaause we validate length in [equity_delta_charge_distributor]
            let b_as_idx_plus_1 = unsafe{ bucket_df["b"].get_unchecked(0)};
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st)=>{ st.parse::<usize>()
                    //.map(|r| if r<rho_diff_curve.len() {Ok(r)}else{Ok(1)})
                    .unwrap_or_else(|_|1)}
            ,   _=>1};
            let rho_diff_curve = rho_diff_curve.get(b_as_idx_plus_1-1).unwrap_or_else(||&0.);

            // CALCULATE Kb Sb for a bucket
            let buck_kb_sb = bucket_kb_sb_single_type(
                bucket_df,
                rho_same_curve,
                *rho_diff_curve,
                scenario_fn,
                columns,
                None,
                special_bucket
                 );
            let mut res = arc_mtx.lock().unwrap();
            res[b_as_idx_plus_1-1] = buck_kb_sb;
            }
        );
        let reskbs_sbs: Result<Vec<((String, f64), f64)>> = Arc::try_unwrap(arc_mtx)
            .map_err(|_|PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
            .into_inner()
            .map_err(|_|PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
            .into_iter()
            .collect();
        
        let buckets_kbs_sbs = reskbs_sbs?;
        let (buckets_kbs, sbs): (Vec<(String, f64)>, Vec<f64>) = buckets_kbs_sbs.into_iter().unzip();
        let (_buckets, kbs): (Vec<String>, Vec<f64>) = buckets_kbs.into_iter().unzip();

        Ok( kbs.into_iter().zip(sbs.into_iter()).collect() )
    }

/// Girr Delta and Eq Vega share common approach.
/// They have tenors and no RFT (in case of GIRR Infl and XCCY become columns)
pub (crate)fn bucket_kb_sb_single_type<F>(bucket_df: &DataFrame, 
    rho_same_curve: &Array2<f64>, 
    rho_diff_curve: f64,
    scenario_fn: F,
    columns: &[&'static str],
    girr: Option<(f64, f64)>,
    special_bucket: Option<&'static str>
) -> Result<((String, f64), f64)> 
    where F: Fn(f64) -> f64 + Sync + Send + Copy{

    let bucket = unsafe{bucket_df["b"].utf8()?.get_unchecked(0).unwrap_or_else(||"Default")}.to_string();
    let mut sb = 0.; //this is Sb
    let mut var_covar_sum = 0.; // this is pre Kb(var-covar sum) for a single tenor
    let mut cross_tenor = 0.;
    let case1 = scenario_fn(rho_diff_curve); //Same tenor, diff curve
    let yield_df = bucket_df.select(columns)?;
    let all_yield_arr = yield_df
    .fill_null(FillNullStrategy::Zero)?
    .to_ndarray::<Float64Type>()?;

    // EQ Vega, take care of special bucket
    match special_bucket {
        Some(x) if x==bucket.as_str() => {
            let abs_sum = all_yield_arr.iter()
            .map(|x|x.abs())
            .sum::<f64>();
            return Ok(((x.to_string(), abs_sum),abs_sum))
        },
        _ => (),
    };


    if let Some((rho_infl, rho_xccy)) = girr{
        let xccy: f64 = bucket_df["XCCY"].sum().unwrap_or_else(||0.);
        // 21.8.2.b
        let infl: f64 = bucket_df["Inflation"].sum().unwrap_or_else(||0.);
        sb = sb + xccy+infl;
        
        var_covar_sum = var_covar_sum + xccy.powi(2) + infl.powi(2);
        let case2 = scenario_fn(rho_infl); // Yield (any tenor) vs Infl
        let case3 = scenario_fn(rho_xccy); // Yield (any tenor) vs XCCY
        var_covar_sum += case2*2f64*infl*all_yield_arr.sum();
        var_covar_sum += case3*2f64*xccy*all_yield_arr.sum();
    }

        
    for (i, c1) in columns.iter().enumerate() {
        let _i = 1;
        let (sum, var_covar) = var_covar_sum_single(&yield_df[*c1], case1)?;
        sb+=sum;
        var_covar_sum += var_covar;

        if columns[(i+1)..].is_empty(){continue};
        let mut current_yield_arr = yield_df[*c1].f64()?.to_ndarray()?.to_owned();
        let next_yields_arr = all_yield_arr.slice(s![..,(i+1)..]);

        let mut tenor_rho = Array2::<f64>::zeros(next_yields_arr.raw_dim());
        tenor_rho.axis_iter_mut(Axis(1))
        .enumerate()
        .for_each(|(col, mut x)|{
            let cross_tenor_rho = unsafe{ rho_same_curve.uget((i, i+1+col)) };
            x.fill(*cross_tenor_rho);
        });
        

        current_yield_arr.indexed_iter_mut()
        .par_bridge()
        .for_each(|(j, v)|{
            if *v != 0f64 {
            let mut uninit_curve_rho = Array2::<f64>::uninit(next_yields_arr.raw_dim());
            let(mut diff_curve1, mut same_curve, mut diff_curve2)
             = uninit_curve_rho.multi_slice_mut((
                s![0..j,..],   
                s![j, ..],     
                s![(j+1)..,..],
                ));
            //assign rho same/diff curve
            diff_curve1.fill(MU::new(rho_diff_curve));
            same_curve.fill(MU::new(1f64));
            diff_curve2.fill(MU::new(rho_diff_curve));
            //initialise
            let curve_rho: Array2<f64>;
            unsafe {
                curve_rho = uninit_curve_rho.assume_init();
            }
            //mult with tenor_rho
            let mut tenor_curve_rho = curve_rho*tenor_rho.view();
            //apply scenario fn
            tenor_curve_rho.par_mapv_inplace(scenario_fn);
            //Now, multiply rho with weighted sensis
            let rho_weighted_next_sens = tenor_curve_rho*next_yields_arr;
            let a = 2f64*(rho_weighted_next_sens*(*v)).sum();
            *v = a;
        }
        });
        cross_tenor += current_yield_arr.sum();
    }
    let kb = (var_covar_sum+cross_tenor).max(0.).sqrt();
    Ok(((bucket, kb), sb))
}