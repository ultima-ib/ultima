use std::mem::MaybeUninit;

use base_engine::prelude::*;
use crate::prelude::*;
use crate::helpers::{ReturnMetric, get_optional_parameter_array, get_optional_parameter};
use crate::sbm::common::{rc_delta_sens, rc_tenor_weighted_sens, across_bucket_agg};

use ndarray::{Array2, Array1};
use polars::prelude::*;
use rayon::iter::ParallelIterator;
use log::warn;

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

///calculate GIRR Vega Low Capital charge
pub(crate) fn girr_vega_charge_low(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Interm Result: GIRR Vega Low Kb 
pub(crate) fn girr_vega_kb_low(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Interm Result: GIRR Vega Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn girr_vega_sb(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}

///calculate GIRR Vega Medium Capital charge
pub(crate) fn girr_vega_charge_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Interm Result: GIRR Vega Medium Kb 
pub(crate) fn girr_vega_kb_medium(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate GIRR Vega Medium Capital charge
pub(crate) fn girr_vega_charge_high(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Interm Result: GIRR Vega Medium Kb 
pub(crate) fn girr_vega_kb_high(op: &OCP) -> Expr {
    girr_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    
    //let girr_delta_gamma = get_optional_parameter(op, format!("girr_delta_gamma{_suffix}").as_ref() as &str, &scenario.girr_delta_gamma);
    let girr_vega_rho = get_optional_parameter_array(op, "base_girr_vega_option_rho", &scenario.vega_rho);

    let girr_vega_gamma = get_optional_parameter(op, "base_girr_vega_option_rho", &scenario.girr_delta_gamma);

    girr_vega_charge( girr_vega_rho, girr_vega_gamma, rtrn)
        
}

fn girr_vega_charge(girr_vega_opt_rho: Array2<f64>, girr_gamma: f64, return_metric: ReturnMetric) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rcat" => columns[0].clone(),
            "rc" =>   columns[1].clone(),
            "b" =>    columns[2].clone(),
            "um" =>   columns[3].clone(),
            "y05" =>  columns[4].clone(),
            "y1" =>   columns[5].clone(),
            "y3" =>   columns[6].clone(),
            "y5" =>   columns[7].clone(),
            "y10" =>  columns[8].clone(),
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Vega"))))
            // .with_column(col("um").fill_nan(col("rft"))) <--> this step is done in preprocessing
            .groupby([col("b"), col("um")])
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
        let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), girr_gamma );
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, res_len)
    }, 
    &[ col("RiskCategory"), col("RiskClass"), col("BucketBCBS"), 
    col("GirrVegaUnderlyingMaturity"), girr_vega_sens_weighted_05y(),
    girr_vega_sens_weighted_1y(), girr_vega_sens_weighted_3y(),
    girr_vega_sens_weighted_5y(), girr_vega_sens_weighted_10y()], 
        GetOutput::from_type(DataType::Float64))
}

fn girr_vega_bucket_kb_sb(lf: LazyFrame, girr_vega_rho: &Array2<f64>, bucket: &str) -> Result<(f64, f64)> {
    let bucket_df = lf
            .filter(col("b").eq(lit(bucket)));

    // Extracting yield curves
    let yield_05um = girr_underlying_maturity_arr(bucket_df.clone(), "0.5Y", bucket)?;
    let yield_1um = girr_underlying_maturity_arr(bucket_df.clone(), "1Y", bucket)?;
    let yield_3um = girr_underlying_maturity_arr(bucket_df.clone(), "3Y", bucket)?;
    let yield_5um = girr_underlying_maturity_arr(bucket_df.clone(), "5Y", bucket)?;
    let yield_10um = girr_underlying_maturity_arr(bucket_df.clone(), "10Y", bucket)?;
    let infl = girr_underlying_maturity_arr(bucket_df.clone(), "Inflation", bucket)?;
    let xccy = girr_underlying_maturity_arr(bucket_df.clone(), "XCCY", bucket)?;

    let mut a = Array1::<f64>::uninit(yield_05um.len() + yield_1um.len() + yield_3um.len()
        + yield_5um.len() + yield_10um.len() + infl.len() + xccy.len());

    let mut i = 0usize;
    for arr in [yield_05um, yield_1um, yield_3um, yield_5um, yield_10um, infl, xccy] {
        let len = arr.len();
        let slice = a.slice_mut(s![i..i+len]);
        arr.move_into_uninit(slice);
        i += len;
    };

    let sens = unsafe{ a.assume_init() };

    let a = sens.dot(girr_vega_rho);

    //21.4.4
    let kb = a.dot(&sens)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = sens.sum();

    Ok((kb, sb))
}

/// Returns Array1 of shape 5 which represents 5 option mat tenors for a given 
/// girr maturity
pub(crate) fn girr_underlying_maturity_arr(lf: LazyFrame, mat: &str, b: &str) -> Result<Array1<f64>> {
    Ok(lf.filter(col("um").eq(lit(mat)))
        .select([col("y05"), col("y1"), col("y3"),
                 col("y5"), col("y10")])
        .collect()?
        .to_ndarray::<Float64Type>()?
        .into_shape(5).unwrap_or_else(|_|{
            //warn!("For bucket: {b}, GirrVegaUnderlyingMaturity {mat} not found. Zero's will be used");
            Array1::<f64>::zeros(5)
        }))
}

pub(crate) fn girr_vega_rho() -> Array2<f64> {
    let base = option_maturity_rho();
    let res: Array2<f64>;
    let mut arr = Array2::<f64>::uninit((35, 35));
    arr.exact_chunks_mut((5,5))
    .into_iter()
    .enumerate()
    //.par_bridge()
    .for_each(|(i, chunk)|{
        //we have total 7(chunks per row)*7(chunks per col) = 49 chunks
        let row_id = i/7; //eg 27usize/5usize = 5usize
        let col_id = i%7; //eg 27usize % 5usize = 2usize
        if row_id==col_id {
            base.to_owned().move_into_uninit(chunk)
        } else if  (row_id==6) | (col_id==6)  {
            (&base*0.).move_into_uninit(chunk)
        } else if  (row_id==5) | (col_id==5)  {
            (&base*0.4).move_into_uninit(chunk)
        }
         else {
            let mult = unsafe{ *base.uget((row_id, col_id)) };
            (&base*mult).move_into_uninit(chunk)
        }
    });
    unsafe {
        res = arr.assume_init();
    }
    res
}