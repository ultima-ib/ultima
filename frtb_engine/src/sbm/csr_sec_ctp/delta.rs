//! CSR Sec CTP Delta Calculations
//! 
use base_engine::prelude::*;
use crate::helpers::*;
use sbm::csr_nonsec::delta::csr_nonsec_delta_charge;

use crate::sbm::common::*;
use crate::prelude::*;
use polars::prelude::*;


pub fn total_csr_sec_ctp_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("CSR_Sec_CTP", "Delta", total_delta_sens())
}
/// Helper functions

fn csr_sec_ctp_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_05Y", "SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_1Y","SensWeights", 0)
}
fn csr_sec_ctp_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_3Y","SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_5Y","SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_10Y","SensWeights",0)
}

//CRR2
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_05y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_05Y", "SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_1y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_1Y","SensWeightsCRR2", 0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_3y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_3Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_5y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_5Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_10y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_Sec_CTP", "Sensitivity_10Y","SensWeightsCRR2",0)
}

/// Total weighted CSR non-Sec Delta
/// Not used in calculation
pub(crate) fn csr_sec_ctp_delta_sens_weighted(op: &OCP) -> Expr {

    let juri: Jurisdiction = get_jurisdiction(op);
    
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => csr_sec_ctp_delta_sens_weighted_05y_crr2().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_1y_crr2().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_3y_crr2().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_5y_crr2().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_10y_crr2().fill_null(0.),
        Jurisdiction::BCBS => csr_sec_ctp_delta_sens_weighted_05y_bcbs().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_1y_bcbs().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_3y_bcbs().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_5y_bcbs().fill_null(0.)
               + csr_sec_ctp_delta_sens_weighted_10y_bcbs().fill_null(0.),
    } 
}

///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_sec_ctp_delta_charge_low(op: &OCP) -> Expr {
    csr_sec_ctp_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_sec_ctp_delta_charge_medium(op: &OCP) -> Expr {
    csr_sec_ctp_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)  
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_sec_ctp_delta_charge_high(op: &OCP) -> Expr {
    csr_sec_ctp_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
/// calls csr_nonsec_delta_charge because the calculation is identical
fn csr_sec_ctp_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    
    // First, obtaining parameters specific to jurisdiciton
    let (weight, bucket_col, name_rho_vec, 
        gamma_rating, gamma_sector,
        n_buckets, special_bucket) =
         match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (col("SensWeightsCRR2"),
        col("BucketCRR2"),
        Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
        &scenario.base_csr_ctp_gamma_rating_crr2, &scenario.base_csr_ctp_gamma_sector_crr2,
        18usize, Option::<usize>::None,
        ),
        Jurisdiction::BCBS=>
        (
        col("SensWeights"),
        col("BucketBCBS"),
        Vec::from(scenario.base_csr_ctp_rho_name_bcbs),
        &scenario.base_csr_ctp_gamma_rating, &scenario.base_csr_ctp_gamma_sector,
        16usize, Option::<usize>::None,
        )
        };

    // Checking if request contains overrides
    let base_csr_ctp_rho_tenor = get_optional_parameter_array(op,"base_csr_ctp_tenor_rho", 
    &scenario.base_csr_ctp_rho_tenor);

    let name_rho_vec = get_optional_parameter_vec(op,"base_csr_ctp_diff_name_rho_per_bucket", 
    &name_rho_vec);

    let base_csr_ctp_rho_basis = get_optional_parameter(op,"base_csr_ctp_diff_basis_rho", 
    &scenario.base_csr_nonsec_rho_basis);

    let gamma_rating = get_optional_parameter_array(op,"base_csr_ctp_rating_gamma", 
    gamma_rating);

    let gamma_sector = get_optional_parameter_array(op,"base_csr_ctp_sector_gamma", 
    gamma_sector);


    // CTP calc is identical to nonSec, with the only exception on rho, gamma and number of buckets
    csr_nonsec_delta_charge(weight, 
        base_csr_ctp_rho_tenor,
     name_rho_vec,
    base_csr_ctp_rho_basis, bucket_col, scenario.scenario_fn,
    gamma_rating, gamma_sector, n_buckets, special_bucket, "CSR_Sec_CTP", "Delta")
}