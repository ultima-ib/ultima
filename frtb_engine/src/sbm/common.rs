use base_engine::prelude::*;

use log::warn;
use ndarray::{Array2, Array1, Zip, ArrayView1, Array, Order};
use polars::prelude::*;
use rayon::iter::{ParallelBridge, ParallelIterator, IntoParallelRefMutIterator};
use std::sync::Mutex;

/// Sum of all delta sensis, from spot to 30Y tenor
/// In practice should be used only with filter on RiskClass
/// as combining FX and IR sensis is meaningless
pub fn total_delta_sens() -> Expr {
    // When adding Exprs NULLs have to be filled
    // Otherwise returns NULL
    col("SensitivitySpot").fill_null(0.)
    +col("Sensitivity_025Y").fill_null(0.)
    +col("Sensitivity_05Y").fill_null(0.)
    +col("Sensitivity_1Y").fill_null(0.)
    +col("Sensitivity_2Y").fill_null(0.) 
    +col("Sensitivity_3Y").fill_null(0.)
    +col("Sensitivity_5Y").fill_null(0.)
    +col("Sensitivity_10Y").fill_null(0.)
    +col("Sensitivity_15Y").fill_null(0.)
    +col("Sensitivity_20Y").fill_null(0.)
    +col("Sensitivity_30Y").fill_null(0.)
}

pub(crate) fn total_sens_curv_weighted() -> Expr {
    total_delta_sens()*col("CurvatureRiskWeight")
}
pub(crate) fn cvr_up() -> Expr {
    lit::<f64>(0.) - (col("PnL_Up") - total_sens_curv_weighted() )
}

pub(crate) fn cvr_down() -> Expr {
    lit::<f64>(0.) - (col("PnL_Down") + total_sens_curv_weighted() )
}

pub(crate) fn rc_cvr(rc: &str, dir: CVR)->Expr{
    let cvr = match dir {
        CVR::Up  => cvr_up(),
        CVR::Down => cvr_down(),
    };

    when(col("RiskClass").eq(lit(rc)))
    .then(cvr)
    .otherwise(lit::<f64>(0.))
}

pub(crate) enum CVR {
    Up,
    Down
}

pub fn curv_delta(rc: &str) -> Expr {
    when(col("RiskClass").eq(lit(rc)).and(
        col("PnL_Up").is_not_null().or(col("PnL_Down").is_not_null())
        )
    )
    .then(total_delta_sens())
    .otherwise(lit::<f64>(0.0) )
}

//pub fn rc_pnl

/// WhenThen shouldn't be used inside groupby
/// this works so far
/// but must be VERY careful with this function in calculation
pub fn rc_rcat_sens(rc: &str, rcat: &str, risk: Expr) -> Expr {
    when(
        col("RiskClass").eq(lit(rc)).and(
            col("RiskCategory").eq(lit(rcat)))
    )
    .then(risk) 
    .otherwise(lit::<f64>(0.0))
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

/// Common function used for CSR Sec, Commodity
/// Computes kb and sb efficiently via uninit
pub(crate) fn bucket_kb_sb_chunks<F>(df: LazyFrame, bucket_id: usize, special_bucket: Option<usize>, 
    rho_tenor: &Array2<f64>, rho_diff_rf_bucket: Vec<f64>, rho_diff_rft: f64, scenario_fn: F,
    tenor_cols:Vec<&str>,name_col: &str, basis_col: &str) 
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

    let name_arr = bucket_df[name_col].utf8()?;
    let curve_type_arr = bucket_df[basis_col].utf8()?;

    let rho_name_bucket = rho_diff_rf_bucket[bucket_id-1];

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
        unsafe{ name_arr.get_unchecked(row_id).unwrap_or_else(||"Default") } 
        == unsafe{ name_arr.get_unchecked(col_id).unwrap_or_else(||"Default") }{
            1.
        } else {
            rho_name_bucket
        };

        let basis_rho = if
         unsafe{ curve_type_arr.get_unchecked(row_id).unwrap_or_else(||"Default") } 
         == unsafe{ curve_type_arr.get_unchecked(col_id).unwrap_or_else(||"Default") } {
            1.
        } else {
            rho_diff_rft
        };
        (rho_tenor*name_rho*basis_rho).move_into_uninit(chunk);
        //chunk.assign(&(rho_tenor*name_rho*basis_rho));
    });
    let mut rho: Array2<f64>;
    unsafe {
        rho = arr.assume_init();
    }

    rho.par_mapv_inplace(|el| {scenario_fn(el)});
    // Get rid of NaNs/Zeros before multiplying
    let csr_shaped = ws_arr
            .to_shape((ws_arr.len(), Order::RowMajor) )
            .map_err(|_| PolarsError::ShapeMisMatch("Could not reshape csr arr".into()) )?;
    //21.4.4
    let a = csr_shaped.dot(&rho);
    //21.4.4
    let kb = a.dot(&csr_shaped)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = csr_shaped.sum();
    
    Ok((kb,sb))
}

/// Common way of calculating Kbs and Sbs for all buckets
/// Used for CSR and Commodity
/// df must contain column "b", which represent a bucket
pub(crate) fn all_kbs_sbs<F>(df: DataFrame, tenor_cols:Vec<&str>, n_buckets: usize, 
rho_tenor:&Array2<f64>, bucket_rho_diff_rf: &[f64], rho_base_diff_rft_or_loc: f64, scenario_fn: F,
name_col: &str, basis_col: &str, special_bucket: Option<usize>) 
-> Result<Vec<(f64, f64)>>
where F: Fn(f64) -> f64 + Sync + Send + Copy{

    let mut reskbs_sbs: Vec<Result<(f64, f64)>> = Vec::with_capacity(n_buckets);
    for _ in 0..n_buckets{reskbs_sbs.push(Ok((0., 0.)))};
    let arc_mtx = Arc::new(Mutex::new(reskbs_sbs));
    // Do not iterate over each bukcet. Instead, only iterate over unique buckets
    df["b"]// We know column "b" exists, so will never panic here
    .utf8()?
    .unique()?
    .par_iter()
    .for_each(|b|{
        match b {
            Some(_b) => {
                let mut b_as_idx = _b.parse::<usize>()
                .unwrap_or_else(|_|{
                    warn!("{_b} cannot be parsed into an int representing commodity the bucket. Default to 1.");
                    1usize});
                if b_as_idx > n_buckets {
                    warn!("{_b} is larger than the max bucket for this risk class. Default to 1.");
                    b_as_idx = 1usize;
                }
                // CALCULATE Kb Sb for a bucket
                let a = bucket_kb_sb_chunks(df.clone().lazy(), b_as_idx, special_bucket,
                    rho_tenor, bucket_rho_diff_rf.to_vec(), rho_base_diff_rft_or_loc, scenario_fn,
                    tenor_cols.clone(), name_col, basis_col);
                let mut res = arc_mtx.lock().unwrap();
                res[b_as_idx-1] = a;
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