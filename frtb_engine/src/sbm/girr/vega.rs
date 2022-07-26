use base_engine::prelude::*;
use crate::prelude::*;
use crate::helpers::{ReturnMetric, get_optional_parameter_array, get_optional_parameter};
use crate::sbm::common::{rc_delta_sens, rc_tenor_weighted_sens, across_bucket_agg};

use ndarray::{Array2, Array1, ArrayView1, Array};
use polars::prelude::*;
use rayon::iter::ParallelIterator;

pub fn total_ir_vega_sens (_: &OCP) -> Expr {
    rc_delta_sens("GIRR", "Vega")
}

fn girr_vega_sens_weighted_05y() -> Expr {
    rc_tenor_weighted_sens("Vega","GIRR", "Sensitivity_05Y", "SensWeights",0)
}
fn girr_vega_sens_weighted_1y() -> Expr {
    rc_tenor_weighted_sens("Vega","GIRR", "Sensitivity_1Y","SensWeights", 0)
}
fn girr_vega_sens_weighted_3y() -> Expr {
    rc_tenor_weighted_sens("Vega","GIRR", "Sensitivity_3Y","SensWeights",0)
}
fn girr_vega_sens_weighted_5y() -> Expr {
    rc_tenor_weighted_sens("Vega","GIRR", "Sensitivity_5Y","SensWeights",0)
}
fn girr_vega_sens_weighted_10y() -> Expr {
    rc_tenor_weighted_sens("Vega","GIRR", "Sensitivity_10Y","SensWeights",0)
}

/// Total GIRR Vega Seins
pub(crate) fn girr_vega_sens_weighted(_: &OCP) -> Expr {
    girr_vega_sens_weighted_05y().fill_null(0.)
    + girr_vega_sens_weighted_1y().fill_null(0.)
    + girr_vega_sens_weighted_3y().fill_null(0.)
    + girr_vega_sens_weighted_5y().fill_null(0.)
    + girr_vega_sens_weighted_10y().fill_null(0.)
}

///calculate GIRR Vega Medium Capital charge
pub(crate) fn girr_vega_charge_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Interm Result: GIRR Vega Medium Kb 
pub(crate) fn girr_vega_kb_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Interm Result: GIRR Vega Medium Sb 
pub(crate) fn girr_vega_sb_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    
    //let girr_delta_gamma = get_optional_parameter(op, format!("girr_delta_gamma{_suffix}").as_ref() as &str, &scenario.girr_delta_gamma);
    let girr_vega_opt_rho = get_optional_parameter_array(op, "base_girr_vega_option_rho", &scenario.base_vega_option_mat_rho);

    let girr_vega_gamma = get_optional_parameter(op, "base_girr_vega_option_rho", &scenario.girr_delta_gamma);

    girr_vega_charge( girr_vega_opt_rho, girr_vega_gamma, rtrn)
        
}

fn girr_vega_charge(girr_vega_opt_rho: Array2<f64>, girr_vega_gamma: f64, return_metric: ReturnMetric) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rcat" => columns[0].clone(),
            "rc" =>   columns[1].clone(), 
            "rf" =>   columns[2].clone(),
            "rft" =>  columns[3].clone(),
            "b" =>    columns[4].clone(),
            "um" =>   columns[5].clone(),
            "y05" =>  columns[6].clone(),
            "y1" =>   columns[7].clone(),
            "y3" =>   columns[8].clone(),
            "y5" =>   columns[9].clone(),
            "y10" =>  columns[10].clone(),
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Vega"))))
            // .with_column(col("um").fill_nan(col("rft"))) <--> this step to be done in preprocessing
            .groupby([col("b"), col("rf"), col("rft"), col("um")])
            .agg([
                col("y05").sum(),
                col("y1").sum(),
                col("y3").sum(),
                col("y5").sum(),
                col("y10").sum()           
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;
        
        let res_kbs_sbs: Result<Vec<(f64,f64)>> = df["b"].unique()?
            .utf8()?
            .par_iter()
            .map(|b|{
                girr_vega_bucket_kb_sb(df.clone().lazy(), &girr_vega_opt_rho, b.unwrap_or_else(||"Default"))
            })
            .collect();
        let kbs_sbs = res_kbs_sbs?;
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        // Early return Kb or Sb is that is the required metric
        let res_len = columns[0].len();
        match return_metric {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }
        let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), girr_vega_gamma );
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, res_len)
    }, 
    &[ col("RiskCategory"), col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), col("BucketBCBS"), 
    col("GirrVegaUnderlyingMaturity"), girr_vega_sens_weighted_05y(),
    girr_vega_sens_weighted_1y(), girr_vega_sens_weighted_3y(),
    girr_vega_sens_weighted_5y(), girr_vega_sens_weighted_10y()], 
        GetOutput::from_type(DataType::Float64))
}

fn girr_vega_bucket_kb_sb(lf: LazyFrame, girr_vega_opt_rho: &Array2<f64>, bucket: &str) -> Result<(f64, f64)> {
    let bucket_df = lf
            .filter(col("b").eq(lit(bucket)))
            .collect()?;
    dbg!(bucket_df);
    Ok((0., 0.))
}