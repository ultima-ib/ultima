use base_engine::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IndexedParallelIterator};
use rayon::slice::ParallelSliceMut;
use crate::{sbm::common::*, helpers::across_bucket_agg};
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;

/// Total Equity Delta Sens
pub(crate) fn equity_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("Equity")
}

pub(crate) fn equity_delta_sens_weighted (_: &OCP) -> Expr {
    equity_delta_sens_weighted_spot()
}

/// Returns NULL for non Delta risk
pub(crate) fn equity_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens("Delta", "Equity", "SensitivitySpot","SensWeights", 0)
}

///calculate Equity Delta High Capital charge
pub(crate) fn equity_delta_charge_high(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

///calculate Equity Delta Medium Capital charge
pub(crate) fn equity_delta_charge_medium(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)
}


///calculate Equity Delta Medium Capital charge
pub(crate) fn equity_delta_charge_low(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)
}

fn equity_delta_charge_distributor(_: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    equity_delta_charge(&scenario.eq_gamma, scenario.base_eq_rho_bucket, scenario.base_eq_rho_mult, scenario.scenario_fn)
}

///calculate FX Delta Capital charge
fn equity_delta_charge<F>(gamma: &'static Array2<f64>, eq_rho_bucket: [f64; 13], 
    eq_rho_mult: f64, scenario_fn: F) -> Expr 
    where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,{
    // inner function
    apply_multiple( move |columns| {

        let df = df![
            "b" => columns[0].clone(), 
            "rf" => columns[3].clone(),
            "rft" => columns[2].clone(),
            "dw" => columns[1].clone(),
        ]?;
        
        // 21.4.3 - Netting
        let df = df.lazy()
            .filter(
                // filtering out NULLs here
                // Since equity_delta_sens_weighted_spot returns null for non Delta, non Eq risk
                col("dw").is_not_null()
            )
            .groupby([col("b"), col("rf"), col("rft")])
            .agg([col("dw").sum().alias("dw_sum")])
            .collect()?;
        
        if df.height() == 0 {
            return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) )
        };
        // Compute in parallel the 13 buckets
        let reskbs_sbs: Result<Vec<(f64, f64)>> = (1usize..=13)
        .into_par_iter()
        .map(|bucket| {
            eq_bucket_kb_sb(df.clone(), bucket, eq_rho_bucket, 
            eq_rho_mult, scenario_fn)
        })
        .collect();

        let kbs_sbs = reskbs_sbs?;
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len())

    }, 
    &[ col("BucketBCBS"), equity_delta_sens_weighted_spot(), col("RiskFactorType"), col("RiskFactor") ], 
        GetOutput::from_type(DataType::Float64))
}


fn eq_bucket_kb_sb<F>(df: DataFrame, bucket_id: usize, eq_rho_bucket: [f64; 13], 
    eq_rho_mult: f64, scenario_fn: F) 
    -> Result<(f64, f64)> 
    where F: Fn(f64) -> f64 + Sync + Send + 'static,{
    
        let bucket_df = df.lazy()
                .filter(col("b").eq(lit(bucket_id.to_string())))
                .collect()?;

        if bucket_df.height() == 0 { return Ok((0.,0.)) };

        let dw_sum = bucket_df["dw_sum"]
            .f64()?
            .to_ndarray()?;

        let sb = dw_sum.sum();

        if bucket_id == 11 {
            let kb = dw_sum.map(|x|x.abs()).sum();
            return Ok((kb, sb))
        }

        let buck_rho = eq_rho_bucket[bucket_id-1];

        let mut rho = build_eq_rho(&bucket_df["rf"], &bucket_df["rft"], buck_rho, eq_rho_mult)?;
        dbg!(rho.clone());
        rho.par_mapv_inplace(|el| {scenario_fn(el)});

        //21.4.4
        let a = dw_sum.dot(&rho);

        //21.4.4
        let kb = a.dot(&dw_sum)
            .max(0.)
            .sqrt();
        
        Ok((kb, sb))
}

/// 21.78
/// Used to build both RF based rho and RFT based rho
/// This function is similar to helpers::build_basis_rho but is for a scalar value
fn build_eq_rho(names: &Series, types: &Series, rho_name: f64, rho_type: f64) -> Result<Array2<f64>> {
    // Note: we never have same type AND same issuer since these were netted
    // ie never APPspot APPspot
    // APPLspot APPLrepo is 0.999*1 because spot != repo(0.999), and APP APP (1)
    // APPLspot GOOGspot/APPLrepo GOOGrepo 
    // is 1*0.25 because spot == spot (1) and Goog != App (0.25)
    // Apprepo Googspot is 0.999*0.25 because repo != spot and App != Goog (0.25)
    // Hence, it's sufficient to build two matrixes:
    // 1 based on rft and 2 based on rf 
    let ln = names.len();
    let chunkarr_name = names.utf8()?;
    let chunkarr_type = types.utf8()?;
    
    //let mut all_rhos_vec: Vec<f64> = Vec::with_capacity(ln*ln);
    let mut all_rhos_vec: Vec<f64> = vec![0.;ln*ln];

    all_rhos_vec
    .par_chunks_exact_mut(ln)
    .enumerate()
    .for_each(|(i, res)| {
        // First curve
        let rf_i = unsafe{ chunkarr_name.get_unchecked(i).unwrap_or_else(||"Default") };
        //Similarly, second curve
        let rft_i = unsafe{ chunkarr_type.get_unchecked(i).unwrap_or_else(||"Default") };

        res.iter_mut()
        .zip(chunkarr_name)
        .zip(chunkarr_type)
        .for_each(|((r, name2), type2)|{
            let _rho_name = if rf_i == name2.unwrap_or_else(||"Default") {
                1.
            } else {rho_name};
            let _rho_type = if rft_i == type2.unwrap_or_else(||"Default") {
                1.
            } else {rho_type};
            *r = _rho_name*_rho_type
        });
    });

    Array2::<f64>::from_shape_vec((ln, ln), all_rhos_vec)
        .map_err(|_| PolarsError::ShapeMisMatch("Could not build Eq RF Rho. Invalid Shape".into()))
    
}