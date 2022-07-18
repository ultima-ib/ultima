//! CSR Sec CTP Delta Calculations
//! 
use base_engine::prelude::*;
use crate::helpers::*;
use sbm::csr_nonsec::delta::csr_nonsec_delta_charge;

use crate::sbm::common::*;
use crate::prelude::*;
use polars::prelude::*;


pub fn total_csr_sec_ctp_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("CSR_secCTP")
}
/// Helper functions

fn csr_sec_ctp_delta_sens_weighted_05y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_05Y", "SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_1y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_1Y","SensWeights", 0)
}
fn csr_sec_ctp_delta_sens_weighted_3y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_3Y","SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_5y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_5Y","SensWeights",0)
}
fn csr_sec_ctp_delta_sens_weighted_10y_bcbs() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_10Y","SensWeights",0)
}

//CRR2
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_05y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_05Y", "SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_1y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_1Y","SensWeightsCRR2", 0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_3y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_3Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_5y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_5Y","SensWeightsCRR2",0)
}
#[cfg(feature = "CRR2")]
fn csr_sec_ctp_delta_sens_weighted_10y_crr2() -> Expr {
    rc_tenor_weighted_sens("Delta","CSR_secCTP", "Sensitivity_10Y","SensWeightsCRR2",0)
}

/// Total CSR non-Sec Delta
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
fn csr_sec_ctp_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    
    let (y05, y1, y3, y5, y10, bucket_col, name_rho_vec, 
        gamma_rating, gamma_sector,
        n_buckets, special_bucket) =
         match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (csr_sec_ctp_delta_sens_weighted_05y_crr2(),
        csr_sec_ctp_delta_sens_weighted_1y_crr2(),
        csr_sec_ctp_delta_sens_weighted_3y_crr2(),
        csr_sec_ctp_delta_sens_weighted_5y_crr2(),
        csr_sec_ctp_delta_sens_weighted_10y_crr2(),
        col("BucketCRR2"),
        Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
        &scenario.base_csr_ctp_gamma_rating_crr2, &scenario.base_csr_ctp_gamma_sector_crr2,
        18usize, Option::<usize>::None,
        ),
        Jurisdiction::BCBS=>
        (csr_sec_ctp_delta_sens_weighted_05y_bcbs(),
        csr_sec_ctp_delta_sens_weighted_1y_bcbs(),
        csr_sec_ctp_delta_sens_weighted_3y_bcbs(),
        csr_sec_ctp_delta_sens_weighted_5y_bcbs(),
        csr_sec_ctp_delta_sens_weighted_10y_bcbs(),
        col("BucketBCBS"),
        Vec::from(scenario.base_csr_ctp_rho_name_bcbs),
        &scenario.base_csr_ctp_gamma_rating, &scenario.base_csr_ctp_gamma_sector,
        16usize, Option::<usize>::None,
        )
        };

        // CTP calc is identical to nonSec, with the only exception on rho, gamma and number of buckets
        csr_nonsec_delta_charge(y05, y1, y3, y5, y10, 
        &scenario.base_csr_nonsec_rho_tenor, name_rho_vec,
        scenario.base_csr_ctp_rho_basis, bucket_col, scenario.scenario_fn,
        gamma_rating, gamma_sector, n_buckets, special_bucket, "CSR_secCTP", "Delta")
}