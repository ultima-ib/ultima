use base_engine::prelude::*;

use log::warn;
use ndarray::{Array2, Array1, Zip, ArrayView1, Array, Order, s};
use polars::prelude::*;
use rayon::{iter::{ParallelBridge, ParallelIterator, IntoParallelRefMutIterator}, prelude::IntoParallelRefIterator};
use std::{sync::Mutex, iter};
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
pub(crate) fn bucket_kb_sb<F>(df: LazyFrame, bucket_id: usize, special_bucket: Option<usize>, 
    rho_tenor: &Array2<f64>, name_col: &str, rho_diff_rf_bucket: &[f64], 
    basis_col: Option<&str>, rho_diff_rft: Option<f64>,  scenario_fn: F, tenor_cols:Vec<&str>, ) 
-> Result<(f64, f64)> 
where F: Fn(f64) -> f64 + Sync + Send,
{
    let bucket_df = df
            .filter(col("b").eq(lit(bucket_id.to_string())))
            .collect()?;
        
    dbg!(bucket_id);

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
    let mut tmp: Vec<usize> = Vec::with_capacity(sbs.len());
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

pub(crate) fn kb_plus_minus(srs: &Series) -> Result<Vec<f64>>{
    Ok(srs
    .f64()?
    .into_iter()
    .map(|cv_up|
        f64::max(cv_up.unwrap_or_else(||0.), 0.)
    )
    .collect())
}

pub(crate) fn kbs_sbs_curvature(kb_plus: Vec<f64>,kb_minus: Vec<f64>, cvr_up: &Series, cvr_down: &Series) -> Result<(Vec<f64>, Vec<f64>)> {
    let kbs_sbs: Vec<(f64, f64)> = kb_plus.into_iter()
        .zip(kb_minus.into_iter())
        .zip(cvr_up.f64()?.into_iter())
        .zip(cvr_down.f64()?.into_iter())
        .map(|(((kb_p, kb_m), cv_up), cv_down)|
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

/// ALternative to [bucket_kb_sb]
/// Instead of iterating over each chunk, for each i in rf.zip(rft) , iterate over rf.zip(rft),skip(i)
/// This reduces number of iterations by factor of 2, but can't use par_iter. Hence:
/// TODO test on a large portfolio
pub(crate) fn bucket_kb_sb_alt<F>(df: LazyFrame, bucket_id: usize, special_bucket: Option<usize>, 
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
    let ws_arr1 = bucket_df["dw_sum"].f64()?;

    let kb_arc_mtx = Arc::new(Mutex::new(0.));

    let rho_name_bucket = rho_diff_rf_bucket.get(bucket_id-1)
        .map(|x|*x)
        .unwrap_or_else(||0.);

    name_arr.into_iter()
    .zip(curve_type_arr.into_iter().chain(iter::repeat(None)))
    .zip(ws_arr1.into_iter())
    .enumerate()
    .par_bridge()
    .for_each(|(i, ((rf, rft), val))|{
        let val = val.unwrap_or_default();
        let mut res = val.powi(2);
        for  ((rf1, rft1), val1) in name_arr.into_iter()
            .zip(curve_type_arr.into_iter().chain(iter::repeat(None)))
            .zip(ws_arr1.into_iter())
            .skip(i) {
                let val1 = val1.unwrap_or_default();
                let name_rho = if rf == rf1 { 1.} else { rho_name_bucket };
                let basis_rho = if rft == rft1 { 1. } else { rho_diff_rft.expect("basis_col was provided, hence expect rho_diff_rft") } ;
                let rho_val_val1 = 2.*scenario_fn(name_rho*basis_rho)*val*val1;
                res += rho_val_val1;
            }
        *kb_arc_mtx.lock().unwrap() += res;
    });let sb = ws_arr1.sum().unwrap_or_default();
    

    let kb = Arc::try_unwrap(kb_arc_mtx)
        .map_err(|_|PolarsError::ComputeError("Couldn't unwrap Arc".into()))?
        .into_inner()
        .map_err(|_|PolarsError::ComputeError("Couldn't get Mutex inner".into()))?
        .max(0.)
        .sqrt();    
    
    Ok((kb,sb))
}


pub(crate) fn all_kbs_sbs_eq<F>(df: DataFrame, n_buckets: usize, bucket_rho_diff_rf: &[f64], 
    rho_base_diff_rft_or_loc: f64, 
    scenario_fn: F, special_bucket: Option<usize>,
    cols_by_tenor: &[(&str, &str)],
    dtenor: Option<f64>,
    ) 
    -> Result<Vec<(f64, f64)>>
    where F: Fn(f64) -> f64 + Sync + Send + Copy{
    
        // vec![Ok((0., 0.)); n_buckets]
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
            let b_as_idx_plus_1 = match b_as_idx_plus_1 {
                AnyValue::Utf8(st)=>{ st.parse::<usize>().unwrap_or_else(|_|1)}
            ,   _=>1};

            // CALCULATE Kb Sb for a bucket
            let buck_kb_sb = bucket_kb_sb_eq(bucket_df, b_as_idx_plus_1, special_bucket,
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
/// * `df` - A "pivoted" Dataframe. Rows are names(RFs), columns are 2(for each RFT) x ntenors
/// * `tenor_cols` , all columns are expected to .f64(), otherwise we will panic
pub(crate) fn bucket_kb_sb_eq<F>(df: &DataFrame, bucket_id: usize, special_bucket: Option<usize>,
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
            
            let (pre_kb, pre_sb) = var_covar_sum_fn(&df[*c1], &df[*c2], rho_case1, rho_case2, rho_case3);
            sb += pre_sb;
            var_covar_sum += pre_kb;
            // Now, if there are any other tenors, we need to account for them
            // First, check dtenor was provided
            match dtenor {
                Some(dt)=>{
                    let case1 =  scenario_fn(dt);
                    let case2 =  scenario_fn(dt*rho_diff_rft);
                    let case3 =  scenario_fn(dt*rho_name_bucket);
                    let case4 =  scenario_fn(dt*rho_diff_rft*rho_name_bucket);

                    if cols_by_tenor[(t+1)..].is_empty(){continue}else{
                        let mut arr_tenor = df.select([*c1, *c2]).unwrap()
                            .fill_null(FillNullStrategy::Zero)?
                            .to_ndarray::<Float64Type>()?;
                        let dim = arr_tenor.raw_dim();
                        let mut next_tenors_sum = Array2::<f64>::zeros(dim);
                        for (c3, c4) in cols_by_tenor[(t+1)..].iter(){
                            let next_tenor = df.select([*c3, *c4]).unwrap()
                            .fill_null(FillNullStrategy::Zero)?
                            .to_ndarray::<Float64Type>()?;
                            next_tenors_sum = next_tenors_sum + next_tenor;
                        }
                        arr_tenor.indexed_iter_mut()
                        .par_bridge()
                        .for_each(|((i, j), v)|{
                            let anti_j: usize = if j==0{1}else{0};
                            let mut uninit_rho = Array2::<f64>::uninit(dim);
                            let  (mut diff_name_same_type1, mut diff_name_diff_type1, 
                            mut same_name_same_type, mut same_name_diff_type,
                            mut diff_name_same_type2, mut diff_name_diff_type2) =
                            uninit_rho.multi_slice_mut((
                                    s![0..i,j],    s![0..i,anti_j],
                                    s![i, j],      s![i, anti_j],
                                    s![(i+1)..,j], s![(i+1)..,anti_j]
                                    ));
                            
                            diff_name_same_type1.fill(MU::new(case3));
                            diff_name_diff_type1.fill(MU::new(case4));
                            same_name_same_type.fill(MU::new(case1));
                            same_name_diff_type.fill(MU::new(case2));
                            diff_name_same_type2.fill(MU::new(case3));
                            diff_name_diff_type2.fill(MU::new(case4));

                            let rho: Array2<f64>;
                            unsafe {
                                rho = uninit_rho.assume_init();
                            }
                            let a = 2.*(rho*next_tenors_sum.view()*(*v)).sum();
                            *v = a;
                        });
                    
                        cross_tenor += arr_tenor.sum();
                    }
                },
                _=>(),
            }
        };

        let kb = (var_covar_sum+cross_tenor).max(0.).sqrt();
        Ok((kb,sb)) 
    }

/// Calculates Var-Covar Matrix in O(N) for !two distinct risk types!
/// * `srs1` and `srs2` are expected to be ".f64()"
pub(crate) fn var_covar_sum_fn(srs1: &Series, srs2: &Series, rho_case1: f64, rho_case2: f64, rho_case3: f64) -> (f64, f64) {

    let (spot_sum, spot_f1) = var_covar_sum_single(srs1, rho_case1);
    let (repo_sum, repo_f1) = var_covar_sum_single(srs2, rho_case1);
    let pre_sb = repo_sum + spot_sum;
    
    let formula1 = spot_f1 + repo_f1;
    
    let formula2_pt1 = spot_sum*repo_sum*rho_case3;
    // .sum() returns None if array is empty or all NULLs
    let spot_times_repo_sum = (srs1*srs2).f64().unwrap().sum().unwrap_or_else(||0.);

    let formula2_pt2 = rho_case2*spot_times_repo_sum;
    let formula2_pt3 = - rho_case3*spot_times_repo_sum; 
    let formula2 = formula2_pt1+formula2_pt2+formula2_pt3;   
    
    let pre_kb = formula1+2f64*formula2;
    
    (pre_kb, pre_sb)
} 

pub(crate) fn var_covar_sum_single(srs: &Series, rho: f64)->(f64, f64){
    let mut sum_of_sq=0.;
    let mut sum=0.;
    srs.f64().unwrap()
    .into_iter()
    .for_each(|x|
        match x{
            Some(y) => {
                sum_of_sq += y.powi(2);
                sum += y;
            },
            _=>(),
        }
    );
    
    let f1 = 
    sum_of_sq
    - rho*sum_of_sq
    + rho*sum.powi(2);

    (sum, f1)
}