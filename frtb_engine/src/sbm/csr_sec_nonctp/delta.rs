//! CSR Sec non-CTP Delta Calculations
use base_engine::prelude::*;
use crate::sbm::common::*;
use crate::prelude::*;
use polars::prelude::*;
use ndarray::prelude::*;


pub fn total_csr_sec_nonctp_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("CSR_Sec_nonCTP", "Delta", total_delta_sens())
}
/// Helper functions

fn csr_sec_nonctp_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_nonCTP", "Sensitivity_05Y", "SensWeights",0)
}
fn csr_sec_nonctp_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_nonCTP", "Sensitivity_1Y","SensWeights", 0)
}
fn csr_sec_nonctp_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_nonCTP", "Sensitivity_3Y","SensWeights",0)
}
fn csr_sec_nonctp_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_nonCTP", "Sensitivity_5Y","SensWeights",0)
}
fn csr_sec_nonctp_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_nonCTP", "Sensitivity_10Y","SensWeights",0)
}

/// Total weighted CSR Sec nonCTP Delta
/// Not used in calculation
pub(crate) fn csr_sec_nonctp_delta_sens_weighted(_: &OCP) -> Expr {
    csr_sec_nonctp_delta_sens_weighted_05y_bcbs().fill_null(0.)
    + csr_sec_nonctp_delta_sens_weighted_1y_bcbs().fill_null(0.)
    + csr_sec_nonctp_delta_sens_weighted_3y_bcbs().fill_null(0.)
    + csr_sec_nonctp_delta_sens_weighted_5y_bcbs().fill_null(0.)
    + csr_sec_nonctp_delta_sens_weighted_10y_bcbs().fill_null(0.)
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_low(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_medium(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_sec_nonctp_delta_charge_high(op: &OCP) -> Expr {
    csr_sec_nonctp_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
/// calls csr_nonsec_delta_charge because the calculation is identical
fn csr_sec_nonctp_delta_charge_distributor(_: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    
    let (y05, y1, y3, y5, y10, bucket_col, tranche_rho_vec, 
        gamma,
        n_buckets, special_bucket) = 
        (csr_sec_nonctp_delta_sens_weighted_05y_bcbs(),
        csr_sec_nonctp_delta_sens_weighted_1y_bcbs(),
        csr_sec_nonctp_delta_sens_weighted_3y_bcbs(),
        csr_sec_nonctp_delta_sens_weighted_5y_bcbs(),
        csr_sec_nonctp_delta_sens_weighted_10y_bcbs(),
        col("BucketBCBS"),
        Vec::from(scenario.base_csr_sec_nonctp_rho_diff_tranche),
        scenario.csr_sec_nonctp_gamma.to_owned(),
        25usize, Some(25),
        );

        // CTP calc is identical to nonSec, with the only exception on rho, gamma and number of buckets
        csr_sec_nonctp_delta_charge(y05, y1, y3, y5, y10, 
        &scenario.base_csr_sec_nonctp_rho_tenor, tranche_rho_vec,
        scenario.base_csr_sec_nonctp_rho_diff_basis, bucket_col, scenario.scenario_fn,
        gamma, n_buckets, special_bucket, "CSR_Sec_nonCTP", "Delta")
}

pub(crate) fn csr_sec_nonctp_delta_charge<F>(y05: Expr, y1: Expr, y3: Expr, y5: Expr, y10: Expr,
    base_tenor_rho: &'static Array2<f64>, rho_name: Vec<f64>, rho_basis: f64,
    bucket_col: Expr, scenario_fn: F, gamma: Array2<f64>,
    n_buckets: usize, special_bucket: Option<usize>, risk_class: &'static str, risk_cat: &'static str) -> Expr
where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static, {

    apply_multiple( move |columns| {
        //let now = Instant::now();
        let df = df![
            "rcat" =>   columns[9].clone(),
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
            .filter(col("rc").eq(lit(risk_class))
                .and(col("rcat").eq(lit(risk_cat))))
            .groupby([col("b"), col("rf"), col("rft")])
            .agg([
                col("y05").sum(),
                col("y1").sum(),
                col("y3").sum(),
                col("y5").sum(),
                col("y10").sum()           
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;
        // 21.4.4 - 21.5.a
        let tenor_cols = vec!["y05", "y1", "y3", "y5", "y10"];
        
        let kbs_sbs = all_kbs_sbs(df, tenor_cols, n_buckets, &base_tenor_rho,
            "rf", &rho_name, 
            Some("rft"), Some(rho_basis),
             scenario_fn, special_bucket)?;

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
        
        // 21.57 OR 325aj
        // Shape of gamma depends on regulation
        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), bucket_col, 
    y05, y1, y3, y5, y10, col("RiskCategory")], 
    
    GetOutput::from_type(DataType::Float64))

}

