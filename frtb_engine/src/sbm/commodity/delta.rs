//! Commodity Delta Risk Charge
//! TODO Commodity RiskFactor should be of the form ...CCY (same as FX, where CCY is the reporting CCY)
//! 

use base_engine::prelude::*;
use rayon::iter::IntoParallelIterator;
use crate::sbm::common::*;
use crate::helpers::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;

pub fn total_commodity_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("Commodity")
}
/// Helper functions
pub(crate) fn commodity_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens("Delta", "Commodity", "SensitivitySpot","SensWeights", 0)
}
pub(crate) fn commodity_delta_sens_weighted_025y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_025Y","SensWeights", 1)
}
pub(crate) fn commodity_delta_sens_weighted_05y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_05Y","SensWeights", 2)
}
pub(crate) fn commodity_delta_sens_weighted_1y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_1Y","SensWeights", 3)
}
pub(crate) fn commodity_delta_sens_weighted_2y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_2Y","SensWeights", 4)
}
pub(crate) fn commodity_delta_sens_weighted_3y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_3Y","SensWeights", 5)
}
pub(crate) fn commodity_delta_sens_weighted_5y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_5Y","SensWeights", 6)
}
pub(crate) fn commodity_delta_sens_weighted_10y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_10Y","SensWeights", 7)
}
pub(crate) fn commodity_delta_sens_weighted_15y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_15Y","SensWeights", 8)
}
pub(crate) fn commodity_delta_sens_weighted_20y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_20Y", "SensWeights",9)
}
pub(crate) fn commodity_delta_sens_weighted_30y() -> Expr {
    rc_tenor_weighted_sens("Delta","Commodity", "Sensitivity_30Y", "SensWeights",10)
}
/// Total Commodity Delta
pub(crate) fn commodity_delta_sens_weighted(_: &OCP) -> Expr {
    commodity_delta_sens_weighted_spot().fill_null(0.)
    + commodity_delta_sens_weighted_025y().fill_null(0.)
    + commodity_delta_sens_weighted_05y().fill_null(0.)
    + commodity_delta_sens_weighted_1y().fill_null(0.)
    + commodity_delta_sens_weighted_2y().fill_null(0.)
    + commodity_delta_sens_weighted_3y().fill_null(0.)
    + commodity_delta_sens_weighted_5y().fill_null(0.)
    + commodity_delta_sens_weighted_10y().fill_null(0.)
    + commodity_delta_sens_weighted_15y().fill_null(0.)
    + commodity_delta_sens_weighted_20y().fill_null(0.)
    + commodity_delta_sens_weighted_30y().fill_null(0.)
}

///calculate commodity Delta Low Capital charge
pub(crate) fn commodity_delta_charge_low(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)  
}

///calculate commodity Delta Medium Capital charge
pub(crate) fn commodity_delta_charge_medium(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)  
}

///calculate commodity Delta High Capital charge
pub(crate) fn commodity_delta_charge_high(op: &OCP) -> Expr {
    commodity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn commodity_delta_charge_distributor(_: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    // TODO Accept optional parameters from op
    commodity_delta_charge( 
        scenario.base_com_rho_cty,
         &scenario.com_gamma,
        scenario.base_com_rho_basis_diff,
    &scenario.base_com_rho_tenor,
    scenario.scenario_fn)
}

fn commodity_delta_charge<F>(bucket_rho_basis: [f64; 11], com_gamma: &'static Array2<f64>, 
    com_rho_base_diff_loc: f64, rho_tenor: &'static Array2<f64>, scenario_fn: F)
    -> Expr 
    where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,{  
    
        
    apply_multiple( move |columns| {
        
        let df = df![
            "rcat"=>  columns[15].clone(),
            "rc" =>   columns[0].clone(), 
            "rf" =>   columns[1].clone(),
            "loc" =>  columns[2].clone(),
            "b" =>    columns[3].clone(),
            "y0" =>   columns[4].clone(),
            "y025" => columns[5].clone(),
            "y05" =>  columns[6].clone(),
            "y1" =>   columns[7].clone(),
            "y2" =>   columns[8].clone(),
            "y3" =>   columns[9].clone(),
            "y5" =>   columns[10].clone(),
            "y10" =>  columns[11].clone(),
            "y15" =>  columns[12].clone(),
            "y20" =>  columns[13].clone(),
            "y30" =>  columns[14].clone(),
        ]?;
        

        let df = df.lazy()
            .filter(col("rc").eq(lit("Commodity"))
            .and(col("rcat").eq(lit("Delta"))))
            .groupby([col("b"), col("rf"), col("loc")])
            .agg([
                col("y0").sum(),
                col("y025").sum(),
                col("y05").sum(),
                col("y1").sum(),
                col("y2").sum(),
                col("y3").sum(),
                col("y5").sum(),
                col("y10").sum(),
                col("y15").sum(),
                col("y20").sum(),
                col("y30").sum()            
            ])
            .fill_null(lit::<f64>(0.));
            //.collect()?; 
              
        // If no buckets, early return zeros
        //if df.height() == 0 {
        //    return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) );
        //}
        // Compute in parallel the 11 buckets
        let tenor_cols = vec!["y0", "y025", "y05", "y1", "y2","y3", "y5", "y10", "y15", "y20", "y30"];
        
        let reskbs_sbs: Result<Vec<(f64, f64)>> = (1usize..=11)
        .into_par_iter()
        .map(|bucket| {
            bucket_kb_sb_chunks(df.clone(), bucket, None,
            rho_tenor, bucket_rho_basis.to_vec(), com_rho_base_diff_loc, scenario_fn,
            tenor_cols.clone(), "rf", "loc")
        })
        .collect();

        let kbs_sbs = reskbs_sbs?;
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        across_bucket_agg(kbs, sbs, &com_gamma, columns[0].len())

 }, 
    &[ col("RiskClass"), col("RiskFactor"), col("CommodityLocation"), col("BucketBCBS"), 
    commodity_delta_sens_weighted_spot(), commodity_delta_sens_weighted_025y(), commodity_delta_sens_weighted_05y(),
    commodity_delta_sens_weighted_1y(), commodity_delta_sens_weighted_2y(), commodity_delta_sens_weighted_3y(),
    commodity_delta_sens_weighted_5y(), commodity_delta_sens_weighted_10y(), commodity_delta_sens_weighted_15y(),
    commodity_delta_sens_weighted_20y(), commodity_delta_sens_weighted_30y(), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
}

