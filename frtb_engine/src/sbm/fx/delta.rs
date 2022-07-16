use base_engine::prelude::*;
use crate::{prelude::*, helpers::get_jurisdiction};

use polars::prelude::*;
use ndarray::prelude::*;

/// Returns a Series equal to SensitivitySpot with RiskClass == FX and RiskFactor == ...CCY
/// !where CCY is either provided as part of optional parameters,
/// !and if not, then is based on Jurisdiction
pub(crate) fn fx_delta_sens (op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    /*
    let ccy_regex = match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 => "^...EUR$".to_string(),
        _=>"^...USD$".to_string()
    };
    */
    // This works for cases like GBP reporting with BCBS params
    let ccy_regex = op.as_ref()
        .and_then(|map| map.get("reporting_ccy"))
        .and_then(|s| {
            if s.len() == 3 {
                Some(format!("^...{s}$")) 
            } else {
                None
            }
        })
        .unwrap_or({
            match juri{
                #[cfg(feature = "CRR2")]
                Jurisdiction::CRR2 => "^...EUR$".to_string(),
                _=>"^...USD$".to_string()
            }
        });
    

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
        &[col("RiskClass"), col("RiskFactor"), col("SensitivitySpot"), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
}

/// takes CalcParams because we need to know reporting CCY
pub(crate) fn fx_delta_sens_weighted (op: &OCP) -> Expr {
    fx_delta_sens(op) * col("SensWeights").arr().get(0)
}

///calculate FX Delta High Capital charge
/// TODO derive gamma from op or scenario def 
pub(crate) fn fx_delta_charge_high(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

///calculate FX Delta Medium Capital charge
pub(crate) fn fx_delta_charge_medium(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)
}


///calculate FX Delta Medium Capital charge
pub(crate) fn fx_delta_charge_low(op: &OCP) -> Expr {
    fx_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)
}

fn fx_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    let fx_delta_sens_weighted_with_rep_ccy = fx_delta_sens_weighted(op);

    let _suffix = scenario.as_str();

    let fx_delta_gamma = op.as_ref()
        .and_then(|map| map.get(format!("fx_delta_gamma{_suffix}").as_ref() as &str))
        .and_then(|s| s.parse::<f64>().ok() )
        .unwrap_or(scenario.fx_delta_gamma);

    fx_delta_charge(fx_delta_gamma, fx_delta_sens_weighted_with_rep_ccy)
}

///calculate FX Delta Capital charge
fn fx_delta_charge(gamma: f64, fx_delta_sens_weighted: Expr) -> Expr {
    // inner function
    apply_multiple( move |columns| {

        let df = df![
            "rc" => columns[0].clone(), 
            //FX Bucket is RiskFactor
            "rf" => columns[1].clone(),
            "dw" => columns[2].clone(),
        ]?;
        

        let df = df.lazy()
            .filter(col("rc").eq(lit("FX")).and(
                // filtering out NULLs here
                col("dw").is_not_null()
            ))
            .groupby([col("rf")])
            .agg([col("dw").sum().alias("dw_sum")])
            .collect()?;
        
        //21.4.4 |dw_sum| == kb for FX
        //21.4.5.a sb == dw_sum
        let dw_sum = df["dw_sum"].f64()?
            .to_ndarray()?; //Ok since we have filtered out NULLs above

        let mut rho_m = Array::from_elem((dw_sum.len(), dw_sum.len()), gamma );
        let zeros = Array::zeros(dw_sum.len() );
        rho_m.diag_mut().assign(&zeros);

        // 21.4.5 sum { gamma * Sc }
        let x = dw_sum.t().dot(&rho_m);

        // 21.4.5 sum { Sb*x}
        let y = dw_sum.dot(&x);

        // 21.4.5 sum { Kb^2}
        let z = dw_sum.dot(&dw_sum);

        //21.4.5
        let sum = y+z;
        
        //21.4.5 since |dw_sum| == kb => sb_alt == dw_sum == sb 
        let res = sum.sqrt();
        // The function is supposed to return a series of same len as the input, hence we broadcast the result
        let res_arr = Array::from_elem(columns[0].len(), res);
        // if option panics on .unwrap() implement match and use .iter() and then Series from iter
        Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
    }, 
    &[ col("RiskClass"), col("BucketBCBS"), fx_delta_sens_weighted ], 
        GetOutput::from_type(DataType::Float64))
}
