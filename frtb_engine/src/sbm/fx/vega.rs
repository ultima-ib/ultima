use base_engine::prelude::*;
use ndarray::{Array2, Array1, Axis};
use crate::prelude::*;
use crate::helpers::{ReturnMetric, get_optional_parameter_array, get_optional_parameter};
use crate::sbm::common::{rc_rcat_sens, across_bucket_agg, total_vega_sens, SBMChargeType};

use polars::prelude::*;

pub fn total_fx_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "FX", total_vega_sens())
}

pub fn total_fx_vega_sens_weighted (op: &OCP) -> Expr {
    total_fx_vega_sens(op)*col("SensWeights").arr().get(0)
}

/// Sb Low == Sb Medium == Sb High
/// FX Vega Sb is identical to total_fx_vega_sens_weighted
pub(crate) fn fx_vega_sb(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}

/// Interm Result: FX Vega Low Kb 
pub(crate) fn fx_vega_kb_low(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Interm Result: FX Vega Medium Kb 
pub(crate) fn fx_vega_kb_medium(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Interm Result: FX Vega High Kb 
pub(crate) fn fx_vega_kb_high(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate FX Vega Low Capital charge
pub(crate) fn fx_vega_charge_low(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
///calculate FX Vega Medium Capital charge
pub(crate) fn fx_vega_charge_medium(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
///calculate FX Vega High Capital charge
pub(crate) fn fx_vega_charge_high(op: &OCP) -> Expr {
    fx_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn fx_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    
    let fx_vega_rho = get_optional_parameter_array(op, format!("fx_vega_rho{_suffix}").as_str(), &scenario.fx_vega_rho);
    let fx_vega_gamma = get_optional_parameter(op, format!("fx_vega_gamma{_suffix}").as_str(), &scenario.fx_gamma);

    fx_vega_charge( fx_vega_rho, fx_vega_gamma, rtrn)
}

fn fx_vega_charge(fx_vega_rho: Array2<f64>, fx_vega_gamma: f64, rtrn: ReturnMetric) -> Expr {
    apply_multiple( move |columns| {

        let df = df![
            "rcat" => columns[0].clone(),
            "rc" =>   columns[1].clone(),
            "b" =>    columns[2].clone(),
            "y05" =>  columns[3].clone(),
            "y1" =>   columns[4].clone(),
            "y3" =>   columns[5].clone(),
            "y5" =>   columns[6].clone(),
            "y10" =>  columns[7].clone(),
            "wght" => columns[8].clone()
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit("FX")).and(col("rcat").eq(lit("Vega"))))
            .groupby([col("b") ])
            .agg([
                (col("y05")*col("wght")).sum(),
                (col("y1")*col("wght")).sum(),
                (col("y3")*col("wght")).sum(),
                (col("y5")*col("wght")).sum(),
                (col("y10")*col("wght")).sum()           
            ])
            .select(&[col("*").exclude(&["b"])])
            .fill_null(lit::<f64>(0.))
            .collect()?;
        
        let sens = df.to_ndarray::<Float64Type>()?;

        let sbs = sens.sum_axis(Axis(1));

        // Early return Kb or Sb, ie the required metric
        let res_len = columns[0].len();
        match rtrn {
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.sum()).as_slice().unwrap())),
            _ => (),
        }

        // Interm step
        let _kbs = sens.dot(&fx_vega_rho);
        // Actual kbs
        let mut kbs = Array1::<f64>::zeros(sbs.len());
        _kbs.axis_iter(Axis(0))
        .enumerate()
        .for_each(|(i, arr)|{
            let a = unsafe{kbs.uget_mut(i)};
            *a = f64::max( arr.dot( &sens.row(i) ), 0. ).sqrt();
        });
        
        match rtrn {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.sum()).as_slice().unwrap())),
            _ => (),
        }

        let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), fx_vega_gamma );
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::DeltaVega)
    },
    &[col("RiskCategory"), col("RiskClass"), col("BucketBCBS"),
    col("Sensitivity_05Y"),
    col("Sensitivity_1Y"),
    col("Sensitivity_3Y"),
    col("Sensitivity_5Y"),
    col("Sensitivity_10Y"),
    col("SensWeights").arr().get(0)],
    GetOutput::from_type(DataType::Float64)
    )
}