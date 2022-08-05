//! For FX RiskFactor is the original source of risk, could be offshore
//! BucketBCBS/CRR2 to be 

use base_engine::prelude::*;
use crate::{prelude::*, helpers::get_jurisdiction, sbm::common::{SBMChargeType, across_bucket_agg}};

use polars::prelude::*;
use ndarray::prelude::*;

/// This works for cases like GBP reporting with BCBS params
pub(crate) fn ccy_regex(op: &OCP) -> String {
    let juri: Jurisdiction = get_jurisdiction(op);
    op.as_ref()
        .and_then(|map| map.get("reporting_ccy"))
        .and_then(|s| { if s.len() == 3 { Some(format!("^...{s}$")) } else { None } })
        .unwrap_or_else(||{
            match juri{
                #[cfg(feature = "CRR2")]
                Jurisdiction::CRR2 => "^...EUR$".to_string(),
                _=>"^...USD$".to_string()
            }
        })
}

/// Returns a Series equal to SensitivitySpot with RiskClass == FX and RiskFactor == ...CCY
/// !where CCY is either provided as part of optional parameters,
/// !and if not, then is based on Jurisdiction
pub(crate) fn fx_delta_sens_repccy (op: &OCP) -> Expr {
    let ccy_regex = ccy_regex(op);
    
    apply_multiple( move |columns| {
        let mask1 = columns[0]
            .utf8()?
            .equal("FX");

        // function to take rep_ccy as an argument
        let mask2 = columns[1]
            .utf8()?
            .contains(ccy_regex.as_str())?;
        
        // function to take rep_ccy as an argument
        let mask3 = columns[3]
            .utf8()?
            .equal("Delta");
        
        // Set delta's which don't match mask1 or mask2 to None (ie NaN)
        let delta = columns[2]
            .f64()?
            .set(&!(mask1&mask2&mask3), None)?;

        Ok(delta.into_series())
    }, 
        &[col("RiskClass"), col("BucketBCBS"), col("SensitivitySpot"), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
}

/// takes CalcParams because we need to know reporting CCY
pub(crate) fn fx_delta_sens_weighted (op: &OCP) -> Expr {
    fx_delta_sens_repccy(op) * col("SensWeights").arr().get(0)
}
///calculate FX Delta Sb, same for all scenarios
pub(crate) fn fx_delta_sb(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)
}

///calculate FX Delta Kb, same for all scenarios
pub(crate) fn fx_delta_kb(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)
}

///calculate FX Delta High Capital charge
pub(crate) fn fx_delta_charge_high(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate FX Delta Medium Capital charge
pub(crate) fn fx_delta_charge_medium(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

///calculate FX Delta Low Capital charge
pub(crate) fn fx_delta_charge_low(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn fx_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let fx_delta_sens_weighted_with_rep_ccy = fx_delta_sens_weighted(op);

    let _suffix = scenario.as_str();

    let fx_delta_gamma = get_optional_parameter(op, format!("fx_delta_gamma{_suffix}").as_ref() as &str, &scenario.fx_gamma);

    fx_delta_charge(fx_delta_gamma, fx_delta_sens_weighted_with_rep_ccy, rtrn)
}

///calculate FX Delta Capital charge
fn fx_delta_charge(gamma: f64, fx_delta_sens_weighted: Expr, rtrn: ReturnMetric) -> Expr {
    // inner function
    apply_multiple( move |columns| {

        let df = df![
            "rc" => columns[0].clone(), 
            //FX Bucket is RiskFactor
            "rf" => columns[1].clone(),
            "dw" => columns[2].clone(),
        ]?;
        

        let df = df.lazy()
            .filter(
                // filtering out NULLs here, as non FX non Delta were set to NULL
                col("dw").is_not_null()
            )
            .groupby([col("rf")])
            .agg([col("dw").sum().alias("dw_sum")])
            .collect()?;
        
        //21.4.4 |dw_sum| == kb for FX
        //21.4.5.a sb == dw_sum
        let dw_sum = df["dw_sum"].f64()?
            .to_ndarray()?; //Ok since we have filtered out NULLs above
        // Early return Kb or Sb, ie the required metric
        let res_len = columns[0].len();
        match rtrn {
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, dw_sum.sum()).as_slice().unwrap())),
            _ => (),
        }
        let kbs: Array1<f64> = dw_sum.iter().map(|x|x.abs()).collect();
        match rtrn {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.sum()).as_slice().unwrap())),
            _ => (),
        }

        let mut gamma = Array::from_elem((dw_sum.len(), dw_sum.len()), gamma );
        let zeros = Array::zeros(dw_sum.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, dw_sum.to_owned(), &gamma, res_len, SBMChargeType::DeltaVega)
    }, 
    &[ col("RiskClass"), col("BucketBCBS"), fx_delta_sens_weighted ], 
        GetOutput::from_type(DataType::Float64))
}
