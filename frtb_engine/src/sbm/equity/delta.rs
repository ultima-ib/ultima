//! Main Equity Delta Calculator
//! For construction of Rho Note:
//! We never have same type AND same issuer since these were netted
//! ie never APPspot APPspot
//! APPLspot APPLrepo is 0.999*1 because spot != repo(0.999), and APP APP (1)
//! APPLspot GOOGspot/APPLrepo GOOGrepo 
//! is 1*0.25 because spot == spot (1) and Goog != App (0.25)
//! Apprepo Googspot is 0.999*0.25 because repo != spot and App != Goog (0.25)
//! Hence, it's sufficient to build two matrixes:
//! 1 based on rft and 2 based on rf 

use base_engine::prelude::*;
use crate::sbm::common::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;

/// Total Equity Delta Sens
pub(crate) fn equity_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "Equity", col("SensitivitySpot"))
}
// wrapper of equity_delta_sens_weighted_spot which takes a param o
pub(crate) fn equity_delta_sens_weighted (_: &OCP) -> Expr {
    equity_delta_sens_weighted_spot()
}
/// 
pub(crate) fn equity_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens("Delta", "Equity", "SensitivitySpot","SensWeights", 0)
}
/// Interm Result: Equity Delta Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn eq_delta_sb(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}
/// Interm Result: Equity Kb Low
pub(crate) fn eq_delta_kb_low(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}
/// Interm Result: Equity Kb Medium
pub(crate) fn eq_delta_kb_medium(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
/// Interm Result: Equity Kb High
pub(crate) fn eq_delta_kb_high(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate Equity Delta High Capital charge
pub(crate) fn equity_delta_charge_high(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate Equity Delta Medium Capital charge
pub(crate) fn equity_delta_charge_medium(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}


///calculate Equity Delta Low Capital charge
pub(crate) fn equity_delta_charge_low(op: &OCP) -> Expr {
    equity_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn equity_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    let eq_gamma = get_optional_parameter_array(op, format!("eq_delta_gamma{_suffix}").as_str(), &scenario.eq_gamma);
    let base_eq_rho_bucket = get_optional_parameter(op, format!("base_eq_rho_bucket{_suffix}").as_str(), &scenario.base_eq_delta_rho_bucket);
    let eq_rho_diff_type =  get_optional_parameter(op, format!("eq_rho_diff_type{_suffix}").as_str(), &scenario.base_eq_rho_mult);

    equity_delta_charge(eq_gamma, base_eq_rho_bucket, eq_rho_diff_type, scenario.scenario_fn, rtrn)
}

/// calculate FX Delta Capital charge
fn equity_delta_charge<F>(gamma: Array2<f64>, eq_rho_bucket: [f64; 13], 
    eq_rho_diff_type: f64, scenario_fn: F, rtrn: ReturnMetric) -> Expr 
    where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,{
    // inner function
    apply_multiple( move |columns| {

        let mut df = df![
            "rcat" => columns[0].clone(),
            "rc"   => columns[1].clone(), 
            "b"    => columns[2].clone(), 
            "rf"   => columns[3].clone(),
            "rft"  => columns[4].clone(),
            "d"    => columns[5].clone(),
            "w"    => columns[6].clone(),
        ]?;
        
        // 21.4.3 - Netting
        df = df.lazy()
            .filter(col("rc").eq(lit("Equity")).and(col("rcat").eq(lit("Delta"))))
            .with_columns([
                when(col("rft").eq(lit("EqSpot")))
                .then((col("d")*col("w")).alias("Spot"))
                .otherwise(NULL.lit()),

                when(col("rft").eq(lit("EqRepo")))
                .then((col("d")*col("w")).alias("Repo"))
                .otherwise(NULL.lit())

            ])
            .groupby([col("b"), col("rf")])
            .agg([col("Spot").sum(), col("Repo").sum()])
            .collect()?; 

        if df.height() == 0 { return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) )};

        // 21.78
        let kbs_sbs = all_kbs_sbs_two_types(
            df,
            13,
            &eq_rho_bucket,
             eq_rho_diff_type,
            scenario_fn, 
            Some(11),
        &[("Spot", "Repo")], 
        None)?;

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        // Early return Kb or Sb is that is the required metric
        let res_len = columns[0].len();
        //let a = Float64Chunked::from_vec("Res", vec![kbs.iter().sum();res_len]);
        match rtrn {
            ReturnMetric::Kb => return Ok(Float64Chunked::from_vec("Res", vec![kbs.iter().sum();res_len]).into_series()),
            ReturnMetric::Sb => return Ok(Float64Chunked::from_vec("Res", vec![sbs.iter().sum();res_len]).into_series()),
            _ => (),
        }

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)

    }, 
    &[ 
        col("RiskCategory"),
        col("RiskClass"),
        col("BucketBCBS"), 
        col("RiskFactor"),
        col("RiskFactorType"),
        col("SensitivitySpot"),
        col("SensWeights").arr().get(0),
        //equity_delta_sens_weighted_spot()
        ], 
        GetOutput::from_type(DataType::Float64))
}
