//! CSR non-Sec Delta Calculations
use base_engine::prelude::*;
use crate::helpers::*;
use crate::sbm::common::*;

use crate::prelude::*;
use polars::prelude::*;
use ndarray::prelude::*;


pub fn total_csr_nonsec_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("CSR_nonSec", "Delta", total_delta_sens())
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
    let _suffix = scenario.as_str();
    
    let (y05, y1, y3, y5, y10, bucket_col, name_rho_vec, 
        gamma_rating, gamma_sector,
        n_buckets, special_bucket) =
         match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => (csr_nonsec_delta_sens_weighted_05y_crr2(),
        csr_nonsec_delta_sens_weighted_1y_crr2(),
        csr_nonsec_delta_sens_weighted_3y_crr2(),
        csr_nonsec_delta_sens_weighted_5y_crr2(),
        csr_nonsec_delta_sens_weighted_10y_crr2(),
        col("BucketCRR2"),
        Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
        &scenario.base_csr_nonsec_gamma_rating_crr2, &scenario.base_csr_nonsec_gamma_sector_crr2,
        20usize, Some(18usize)
        ),
        Jurisdiction::BCBS=>
        (csr_nonsec_delta_sens_weighted_05y_bcbs(),
        csr_nonsec_delta_sens_weighted_1y_bcbs(),
        csr_nonsec_delta_sens_weighted_3y_bcbs(),
        csr_nonsec_delta_sens_weighted_5y_bcbs(),
        csr_nonsec_delta_sens_weighted_10y_bcbs(),
        col("BucketBCBS"),
        Vec::from(scenario.base_csr_nonsec_rho_name_bcbs),
        &scenario.base_csr_nonsec_gamma_rating, &scenario.base_csr_nonsec_gamma_sector,
        18, Some(16)
        )
        };

    let base_csr_nonsec_rho_tenor = get_optional_parameter_array(op,"base_csr_nonsec_tenor_rho", 
    &scenario.base_csr_nonsec_rho_tenor);

    let name_rho_vec = get_optional_parameter_vec(op,"base_csr_nonsec_diff_name_rho_per_bucket", 
    &name_rho_vec);

    let base_csr_nonsec_rho_basis = get_optional_parameter(op,"base_csr_nonsec_diff_basis_rho", 
    &scenario.base_csr_nonsec_rho_basis);

    let gamma_rating = get_optional_parameter_array(op,"base_csr_nonsec_rating_gamma", 
    gamma_rating);

    let gamma_sector = get_optional_parameter_array(op,"base_csr_nonsec_sector_gamma", 
    gamma_sector);

    csr_nonsec_delta_charge(y05, y1, y3, y5, y10, 
        base_csr_nonsec_rho_tenor,
         name_rho_vec,
        base_csr_nonsec_rho_basis, 
        bucket_col, scenario.scenario_fn,
        gamma_rating, gamma_sector,
        n_buckets, special_bucket, "CSR_nonSec", "Delta")
}

pub(crate) fn csr_nonsec_delta_charge<F>(y05: Expr, y1: Expr, y3: Expr, y5: Expr, y10: Expr,
    base_tenor_rho: Array2<f64>, rho_name: Vec<f64>, rho_basis: f64,
    bucket_col: Expr, scenario_fn: F, gamma_rating: Array2<f64>, gamma_sector: Array2<f64>,
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
            .with_columns([

                when(col("rft").eq(lit("Bond")))
                .then(col("y05").alias("Bond_y05"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("CDS")))
                .then(col("y05").alias("CDS_y05"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("Bond")))
                .then(col("y1").alias("Bond_y1"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("CDS")))
                .then(col("y1").alias("CDS_y1"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("Bond")))
                .then(col("y3").alias("Bond_y3"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("CDS")))
                .then(col("y3").alias("CDS_y3"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("Bond")))
                .then(col("y5").alias("Bond_y5"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("CDS")))
                .then(col("y5").alias("CDS_y5"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("Bond")))
                .then(col("y10").alias("Bond_y10"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("CDS")))
                .then(col("y10").alias("CDS_y10"))
                .otherwise(NULL.lit()),

            ])
            .groupby([col("b"), col("rf")])
            .agg([
                col("Bond_y05").sum(),
                col("CDS_y05").sum(),
                col("Bond_y1").sum(),
                col("CDS_y1").sum(),
                col("Bond_y3").sum(),
                col("CDS_y3").sum(),
                col("Bond_y5").sum(),
                col("CDS_y5").sum(),
                col("Bond_y10").sum(),
                col("CDS_y10").sum()          
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;

        let kbs_sbs = all_kbs_sbs_eq(df, n_buckets, 
            &rho_name,
            rho_basis, 
            scenario_fn, 
            special_bucket,
            &[("Bond_y05", "CDS_y05"), ("Bond_y1", "CDS_y1"), ("Bond_y3", "CDS_y3"), ("Bond_y5", "CDS_y5"), ("Bond_y10", "CDS_y10")],
            Some(0.65)
        )?; 

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
        
        // 21.57 OR 325aj
        // Shape of gamma depends on regulation
        let mut gamma = (&gamma_sector)*(&gamma_rating);
        gamma.par_mapv_inplace(|el| {scenario_fn(el)});

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), bucket_col, 
    y05, y1, y3, y5, y10, col("RiskCategory")], 
    
    GetOutput::from_type(DataType::Float64))

}


