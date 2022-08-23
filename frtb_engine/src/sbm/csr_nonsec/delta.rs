//! CSR non-Sec Delta Calculations
use base_engine::prelude::*;
use crate::helpers::*;
use crate::sbm::common::*;

use crate::prelude::*;
use polars::prelude::*;
use ndarray::prelude::*;


pub fn total_csr_nonsec_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_nonSec", total_vega_curv_sens())
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

//Interm Results
///Sb is same for each scenario
pub(crate) fn csr_nonsec_delta_sb(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}

pub(crate) fn csr_nonsec_delta_kb_low(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

pub(crate) fn csr_nonsec_delta_kb_medium(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)
}

pub(crate) fn csr_nonsec_delta_kb_high(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)
}


///calculate CSR non-Sec Delta Low Capital charge
pub(crate) fn csr_nonsec_delta_charge_low(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///calculate CSR non-Sec Delta Medium Capital charge
pub(crate) fn csr_nonsec_delta_charge_medium(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///calculate CSR non-Sec Delta High Capital charge
pub(crate) fn csr_nonsec_delta_charge_high(op: &OCP) -> Expr {
    csr_nonsec_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
/// And pass them to the main Delta Charge calculator accordingly
fn csr_nonsec_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();
    let (weight, bucket_col, name_rho_vec, 
        gamma,
        n_buckets, special_bucket) =
        match juri{
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (
            col("SensWeightsCRR2").arr().get(0),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
            &scenario.csr_nonsec_gamma_crr2,
            20usize, 
            Some(18usize)
            ),

            Jurisdiction::BCBS=>
            (
            col("SensWeights").arr().get(0),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_nonsec_rho_name_bcbs),
            &scenario.csr_nonsec_gamma,
            18,
            Some(16)
            )
        };

    let base_csr_nonsec_rho_tenor = get_optional_parameter(op,"base_csr_nonsec_tenor_rho", 
    &scenario.base_csr_nonsec_rho_tenor);

    let name_rho_vec = get_optional_parameter_vec(op,"base_csr_nonsec_diff_name_rho_per_bucket", 
    &name_rho_vec);

    let base_csr_nonsec_rho_basis = get_optional_parameter(op,"base_csr_nonsec_diff_basis_rho", 
    &scenario.base_csr_nonsec_rho_basis);

    let gamma = get_optional_parameter_array(op,"base_csr_nonsec_rating_gamma", 
    gamma);

    csr_nonsec_delta_charge(
        weight,
        base_csr_nonsec_rho_tenor,
         name_rho_vec,
        base_csr_nonsec_rho_basis, 
        bucket_col, scenario.scenario_fn,
        gamma,
        n_buckets, special_bucket, "CSR_nonSec", "Delta",
        rtrn)
}

pub(crate) fn csr_nonsec_delta_charge<F>(
    weight: Expr,
    base_tenor_rho: f64, rho_name: Vec<f64>, rho_basis: f64,
    bucket_col: Expr, scenario_fn: F, 
    gamma: Array2<f64>,
    n_buckets: usize, special_bucket: Option<usize>, risk_class: &'static str, risk_cat: &'static str,
    rtrn: ReturnMetric) -> Expr
where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static, {

    apply_multiple( move |columns| {
        //let now = Instant::now();
        let df = df![
            "rcat" => columns[10].clone(),
            "rc" =>   columns[0].clone(), 
            "rf" =>   columns[1].clone(),
            "rft" =>  columns[2].clone(),
            "b" =>    columns[3].clone(),
            "y05" =>  columns[4].clone(),
            "y1" =>   columns[5].clone(),
            "y3" =>   columns[6].clone(),
            "y5" =>   columns[7].clone(),
            "y10" =>  columns[8].clone(),
            "w" =>    columns[9].clone()
        ]?;        
        
        // concat_lst is actually slower than 
        let df = df.lazy()
            .filter(col("rc").eq(lit(risk_class))
                .and(col("rcat").eq(lit(risk_cat))))
            .with_columns([
                col("y05")*col("w"),
                col("y1")*col("w"),
                col("y3")*col("w"),
                col("y5")*col("w"),
                col("y10")*col("w"),
            ])
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

        let kbs_sbs = all_kbs_sbs_two_types(df, n_buckets, 
            &rho_name,
            rho_basis, 
            scenario_fn, 
            special_bucket,
            &[("Bond_y05", "CDS_y05"), ("Bond_y1", "CDS_y1"), ("Bond_y3", "CDS_y3"), ("Bond_y5", "CDS_y5"), ("Bond_y10", "CDS_y10")],
            Some(base_tenor_rho)
        )?; 

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        let res_len = columns[0].len();
        match rtrn {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), bucket_col, 
    //y05, y1, y3, y5, y10,
    col("Sensitivity_05Y"),
    col("Sensitivity_1Y"),
    col("Sensitivity_3Y"),
    col("Sensitivity_5Y"),
    col("Sensitivity_10Y"),
    weight,// risk weight
     col("RiskCategory")], 
    
    GetOutput::from_type(DataType::Float64))

}


