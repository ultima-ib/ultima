use base_engine::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;
use crate::sbm::common::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::{prelude::*, Zip};

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

fn equity_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    equity_delta_charge(&scenario.eq_gamma, scenario.base_eq_rho_bucket, scenario.base_eq_rho_mult, scenario.scenario_fn)
}

///calculate FX Delta Capital charge
fn equity_delta_charge<F>(gamma: &'static Array2<f64>, eq_rho_bucket: [f64; 13], 
    eq_rho_mult: f64, scenario_fn: F) -> Expr 
    where F: Fn(f64) -> f64 + Sync + Send + 'static,{
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
        
        // 21.4.4
        let mut kbs: [f64; 13] = [0.;13];
        // sb = sum {ws_b}
        let mut sbs: [f64; 13] = [0.;13];

        for bucket_df in df.partition_by(["b"])? {

            let bucket = if let AnyValue::Utf8(b) = unsafe{ bucket_df["b"].get_unchecked(0) } { b } else { unreachable!() };

            let bucket_as_idx = bucket.parse::<usize>().unwrap_or(1);

            let dw_sum = bucket_df["dw_sum"]
                .f64()?
                .to_ndarray()?;
            
            let sb = dw_sum.sum();

            if bucket_as_idx == 11 {
                let kb = dw_sum.map(|x|x.abs()).sum();
                kbs[10] = kb; //kbs[0] stands for bucket number 1, etc etc
                sbs[10] = sb;
                continue;
            }

            let buck_rho = eq_rho_bucket[bucket_as_idx-1];

            let rho_rf = build_eq_rho_base(&bucket_df["rf"], buck_rho)?;
            let rho_rft = build_eq_rho_base(&bucket_df["rft"], eq_rho_mult)?;
            let mut eq_rho = rho_rf*rho_rft;
            eq_rho.par_mapv_inplace(|el| {scenario_fn(el)});

            //21.4.4
            let a = dw_sum.t().dot(&eq_rho);

            //21.4.4
            let kb = a.dot(&dw_sum)
                .max(0.)
                .sqrt();
            
            kbs[bucket_as_idx-1] = kb;
            sbs[bucket_as_idx-1] = sb;
        };

        // If no buckets, early return zeros
        if kbs == [0.;13] && sbs == [0.;13] {
            return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) );
        }
        
        
        let sbs_arr = Array1::from_iter(sbs);
        let kbs_arr = Array1::from_iter(kbs);        

        //21.4.5 sum{ sum {gamma*s_b*s_c} }
        let a = sbs_arr.t().dot(gamma);
        let b = a.dot(&sbs_arr);

        //21.4.5 sum{K-b^2}
        let c = kbs_arr.dot(&kbs_arr);

        let sum = c+b;

        let res = if sum < 0. {
            //21.4.5.b
            let mut sbs_alt = Array1::<f64>::zeros(kbs_arr.raw_dim());
            Zip::from(&mut sbs_alt)
                .and(&sbs_arr)
                .and(&kbs_arr)
                .par_for_each(|alt, &sb, &kb|{
                    let _min = sb.min(kb);
                    *alt = _min.max(-kb);
            });
            //now recalculate capital charge with alternative sb
            //21.4.5 sum{ sum {gamma*s_b*s_c} }
            let a = sbs_alt.t().dot(gamma);
            let b = a.dot(&sbs_alt);
            //21.4.5 sum{K-b^2}
            let c = kbs_arr.dot(&kbs_arr);
            let sum = c+b;
            sum.sqrt()
        } else {
            sum.sqrt()
        };

        // The function is supposed to return a series of same len as the input, hence we broadcast the result
        let res_arr = Array::from_elem(columns[0].len(), res);
        // if option panics on .unwrap() implement match and use .iter() and then Series from iter
        Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
    }, 
    &[ col("BucketBCBS"), equity_delta_sens_weighted_spot(), col("RiskFactorType"), col("RiskFactor") ], 
        GetOutput::from_type(DataType::Float64))
}


/// 21.78
/// Used to build both RF based rho and RFT based rho
/// This function is similar to helpers::build_basis_rho but is for a scalar value
fn build_eq_rho_base(srs: &Series, eq_rho: f64) -> Result<Array2<f64>> {
    // Note: we never have same type AND same issuer since these were netted
    // ie never APPspot APPspot
    // APPLspot APPLrepo is 0.999*1 because spot != repo(0.999), and APP APP (1)
    // APPLspot GOOGspot/APPLrepo GOOGrepo 
    // is 1*0.25 because spot == spot (1) and Goog != App (0.25)
    // Apprepo Googspot is 0.999*0.25 because repo != spot and App != Goog (0.25)
    // Hence, it's sufficient to build two matrixes:
    // 1 based on rft and 2 based on rf 
    let ln = srs.len();
    let _chunkarr = srs.utf8()?;
    let mut all_rhos_vec = Vec::with_capacity(ln*ln);
    for i in 0..ln {
        let rf_i = unsafe{ _chunkarr.get_unchecked(i).unwrap() };
        let mut rf_vec: Vec<f64> = _chunkarr
            .par_iter()
            .map(|x| match x {
                Some(rf2) if rf2==rf_i => 1. ,
                _ => eq_rho
            })
            .collect();
        all_rhos_vec.append(&mut rf_vec);   
    }

    let rho_arr = Array2::<f64>::from_shape_vec((ln, ln), all_rhos_vec)
        .map_err(|_| PolarsError::ShapeMisMatch("Could not build Eq RF Rho. Invalid Shape".into()));
    
    rho_arr
}