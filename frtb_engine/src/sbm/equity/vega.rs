use base_engine::prelude::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;

pub fn total_eq_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "Equity", total_vega_curv_sens())
}

pub fn total_eq_vega_sens_weighted (op: &OCP) -> Expr {
    total_eq_vega_sens(op)*col("SensWeights").arr().get(0)
}
///Interm Result
pub(crate) fn equity_vega_sb(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn equity_vega_kb_low(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_low(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn equity_vega_kb_medium(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_medium(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn equity_vega_kb_high(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Vega Low Capital charge
pub(crate) fn equity_vega_charge_high(op: &OCP) -> Expr {
    equity_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn equity_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    //TODO check
    let eq_gamma = get_optional_parameter_array(op, format!("eq_vega_gamma{_suffix}").as_str(), &scenario.eq_gamma);
    let base_eq_rho_bucket = get_optional_parameter(op, "base_eq_rho_diff_name_bucket", &scenario.base_eq_delta_rho_bucket);
    let eq_vega_rho = get_optional_parameter_array(op, "base_eq_opt_mat_vega_rho", &scenario.base_vega_rho);

    equity_vega_charge(eq_vega_rho, eq_gamma, base_eq_rho_bucket.to_vec(), 
    scenario.scenario_fn, rtrn, Some("11"), "Equity")
}

/// calculate Equity Vega Capital charge. Used for Commodity also
pub(crate) fn equity_vega_charge<F>(opt_mat_rho: Array2<f64>, gamma: Array2<f64>, eq_rho_bucket: Vec<f64>,
     scenario_fn: F, rtrn: ReturnMetric, special_bucket: Option<&'static str>, rc: &'static str) -> Expr 
    where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,{
    // inner function
    apply_multiple( move |columns| {

        let df = df![
            "rcat" => columns[0].clone(),
            "rc" =>   columns[1].clone(),
            "b" =>    columns[2].clone(),
            "rf" =>   columns[3].clone(),
            "y05" =>  columns[4].clone(),
            "y1" =>   columns[5].clone(),
            "y3" =>   columns[6].clone(),
            "y5" =>   columns[7].clone(),
            "y10" =>  columns[8].clone(),
            "wght" => columns[9].clone(),
        ]?;
        
        // 21.4.3 - Netting
        let df = df.lazy()
            .filter(col("rc").eq(lit(rc)).and(col("rcat").eq(lit("Vega"))))
            .groupby([col("b"), col("rf")])
            .agg([
                (col("y05")*col("wght")).sum().alias("y05"),
                (col("y1")*col("wght")).sum().alias("y1"),
                (col("y3")*col("wght")).sum().alias("y3"),
                (col("y5")*col("wght")).sum().alias("y5"),
                (col("y10")*col("wght")).sum().alias("y10")
            ])
            //.fill_null(0.)
            .collect()?;
        
        if df.height() == 0 {
            return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) )
        };
        // Compute present buckets

        // USE all_kbs_sbs here, this helps skipping unnecessary
        // iterations over buckets which are not present
        let kbs_sbs = all_kbs_sbs_single_type(
            df, 
            &opt_mat_rho,
            &eq_rho_bucket,
            scenario_fn,
            &vec!["y05", "y1", "y3", "y5", "y10"],
            special_bucket)?;

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        // Early return Kb or Sb is that is the required metric
        let res_len = columns[0].len();
        match rtrn {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)

    }, 
    &[ 
        col("RiskCategory"), 
        col("RiskClass"), 
        col("BucketBCBS"),
        col("RiskFactor"), 
        col("Sensitivity_05Y"),
        col("Sensitivity_1Y"),
        col("Sensitivity_3Y"),
        col("Sensitivity_5Y"),
        col("Sensitivity_10Y"),
        col("SensWeights").arr().get(0) ], 
        GetOutput::from_type(DataType::Float64))
}
