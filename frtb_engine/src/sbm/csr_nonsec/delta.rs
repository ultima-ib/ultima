//! CSR bib-Sec Delta Calculations
//! 
use base_engine::prelude::*;
use crate::helpers::*;

use ndarray::Order;
use ndarray::parallel::prelude::IntoParallelRefMutIterator;
use rayon::prelude::*;
use crate::sbm::common::*;
use crate::prelude::*;
use polars::prelude::*;
use ndarray::{prelude::*, Zip};
use ndarray::parallel::prelude::ParallelIterator;


pub fn total_csr_nonsec_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("CSR_nonSec")
}
/// Helper functions

fn csr_nonsec_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_05Y", "SensWeights",0)
}
fn csr_nonsec_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_1Y","SensWeights", 0)
}
fn csr_nonsec_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_3Y","SensWeights",0)
}
fn csr_nonsec_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_5Y","SensWeights",0)
}
fn csr_nonsec_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_10Y","SensWeights",0)
}

//CRR2
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_05y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_05Y", "SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_1y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_1Y","SensWeightsCRR2", 0)
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_3y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_3Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_5y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_5Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_nonsec_delta_sens_weighted_10y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_nonSec", "Sensitivity_10Y","SensWeightsCRR2",0)
}

/// Total CSR non-Sec Delta
/// Not used in calculation
pub(crate) fn csr_nonsec_delta_sens_weighted(op: &OCP) -> Expr {

    let juri: Jurisdiction = get_jurisdiction(op);
    
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => csr_nonsec_delta_sens_weighted_05y_crr2().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_1y_crr2().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_3y_crr2().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_5y_crr2().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_10y_crr2().fill_null(0.),
        Jurisdiction::BCBS => csr_nonsec_delta_sens_weighted_05y_bcbs().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_1y_bcbs().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_3y_bcbs().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_5y_bcbs().fill_null(0.)
               + csr_nonsec_delta_sens_weighted_10y_bcbs().fill_null(0.),
    } 
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_nonsec_delta_charge_low(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_nonsec_delta_charge_medium(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_nonsec_delta_charge_high(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
fn csr_nonsec_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    
    let (y05, y1, y3, y5, y10, bucket_col, name_rho_vec, 
        gamma_rating, gamma_sector) =
         match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (csr_nonsec_delta_sens_weighted_05y_crr2(),
        csr_nonsec_delta_sens_weighted_1y_crr2(),
        csr_nonsec_delta_sens_weighted_3y_crr2(),
        csr_nonsec_delta_sens_weighted_5y_crr2(),
        csr_nonsec_delta_sens_weighted_10y_crr2(),
        col("BucketCRR2"),
        Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
        &scenario.base_csr_nonsec_gamma_rating_crr2, &scenario.base_csr_nonsec_gamma_sector_crr2
        ),
        Jurisdiction::BCBS=>
        (csr_nonsec_delta_sens_weighted_05y_bcbs(),
        csr_nonsec_delta_sens_weighted_1y_bcbs(),
        csr_nonsec_delta_sens_weighted_3y_bcbs(),
        csr_nonsec_delta_sens_weighted_5y_bcbs(),
        csr_nonsec_delta_sens_weighted_10y_bcbs(),
        col("BucketBCBS"),
        Vec::from(scenario.base_csr_nonsec_rho_name_bcbs),
        &scenario.base_csr_nonsec_gamma_rating, &scenario.base_csr_nonsec_gamma_sector
        )
        };

    csr_nonsec_delta_charge(juri, y05, y1, y3, y5, y10, 
        &scenario.base_csr_nonsec_rho_tenor, name_rho_vec,
        scenario.base_csr_nonsec_rho_basis, bucket_col, scenario.scenario_fn,
        gamma_rating, gamma_sector)
}

fn csr_nonsec_delta_charge<F>(jurisdiction: Jurisdiction, y05: Expr, y1: Expr, y3: Expr, y5: Expr, y10: Expr,
    base_tenor_rho: &'static Array2<f64>, rho_name: Vec<f64>, rho_basis: f64,
    bucket_col: Expr, scenario_fn: F, gamma_rating: &'static Array2<f64>, gamma_sector: &'static Array2<f64>) -> Expr
where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static, {

    apply_multiple( move |columns| {

        let df = df![
            "rc" =>   columns[0].clone(), 
            "rf" =>   columns[1].clone(),
            "rft" =>  columns[2].clone(),
            "b" =>    columns[3].clone(),
            "y05" =>  columns[4].clone(),
            "y1" =>   columns[5].clone(),
            "y3" =>   columns[6].clone(),
            "y5" =>   columns[7].clone(),
            "y10" =>  columns[8].clone()
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit("CSR_nonSec")))
            .groupby([col("b"), col("rf"), col("rft")])
            .agg([
                col("y05").sum(),
                col("y1").sum(),
                col("y3").sum(),
                col("y5").sum(),
                col("y10").sum()           
            ])
            .collect()?;        

        // 21.4.4
        let (n_buckets, special_bucket) = match jurisdiction {
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (20usize, 18usize),
            Jurisdiction::BCBS => (18usize, 16),
        };
        // 21.4.4 - 21.5.a
        let reskbs_sbs: Result<Vec<(f64, f64)>> = (1usize..=n_buckets)
        .into_par_iter()
        .map(|bucket| {
            csr_bucket_kb_sb(df.clone(), bucket, special_bucket,
             base_tenor_rho, rho_name.clone(), rho_basis, scenario_fn)
        })
        .collect();

        let kbs_sbs = reskbs_sbs?;
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
        
        // 21.57 OR 325aj
        // Shape of gamma depends on regulation
        let mut gamma = gamma_sector*gamma_rating;
        gamma.par_mapv_inplace(|el| {scenario_fn(el)});

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len())
    }, 
    
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), bucket_col, 
    y05, y1, y3, y5, y10], 
    
    GetOutput::from_type(DataType::Float64))

}

fn csr_bucket_kb_sb<F>(df: DataFrame, bucket_id: usize, special_bucket: usize, 
    rho_tenor: &Array2<f64>, rho_name: Vec<f64>, rho_basis: f64, scenario_fn: F) 
-> Result<(f64, f64)> 
where F: Fn(f64) -> f64 + Sync + Send + 'static,{
    let bucket_df = df.lazy()
            .filter(col("b").eq(lit(bucket_id.to_string())))
            .collect()?;
    let n_curves = bucket_df.height();
    if bucket_df.height() == 0 { return Ok((0.,0.)) };

    let mut csr_arr = bucket_df
                .select(["y05", "y1", "y3", "y5", "y10"])?
                .to_ndarray::<Float64Type>()?;
    // 21.56 and 
    if bucket_id == special_bucket {
        csr_arr.par_iter_mut().for_each(|x|*x=x.abs());
        return Ok((csr_arr.sum(),csr_arr.sum()))
    };
    // Reshape in order to perform matrix multiplication
    let csr_shaped = csr_arr
                .to_shape((csr_arr.len(), Order::RowMajor) )
                .map_err(|_| PolarsError::ShapeMisMatch("Could not reshape csr arr".into()) )?;
    // zero/nan indexes can be dropped
    let non_nan_zero_idxs_vec = non_nan_zero_idxs(csr_shaped.view());
    let tenor_rho = build_tenor_rho(n_curves,rho_tenor.view(), &non_nan_zero_idxs_vec)?;
    //21.54.1 and 21.55.1
    let rho_name_bucket = rho_name[bucket_id-1];
    let name_rho = build_basis_rho(5, &bucket_df["rf"], rho_name_bucket, &non_nan_zero_idxs_vec)?;
    //21.54.3 and 21.55.3
    let basis_rho = build_basis_rho(5, &bucket_df["rft"], rho_basis, &non_nan_zero_idxs_vec)?;
    let mut rho = name_rho*tenor_rho*basis_rho;
    //Apply Scenario rho
    rho.par_mapv_inplace(|el| {scenario_fn(el)});
    // Get rid of NaNs/Zeros before multiplying
    let csr_shaped = csr_shaped.select(Axis(0), &non_nan_zero_idxs_vec);
    //21.4.4
    let a = csr_shaped.t().dot(&rho);
    //21.4.4
    let kb = a.dot(&csr_shaped)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = csr_shaped.sum();
    Ok((kb,sb))
}

