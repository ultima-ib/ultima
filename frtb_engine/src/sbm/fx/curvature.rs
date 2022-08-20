use crate::prelude::*;
use ndarray::{Array1, Array2};

use super::delta::ccy_regex;

/// Wrapper used for FX only. Filteres where BucketBCBS is of
/// th form ...CCY where CCY is the reporting CCY
fn risk_filtered_by_ccy(op: &OCP, risk: Expr) -> Expr {
    let ccy_regex = ccy_regex(op);
    risk.apply_many(
        move |columns|{

            let mask = columns[1].utf8()?
            .contains(ccy_regex.as_str())?;

            let res = columns[0].f64()?
            .set(&!mask, None)?;

            Ok(res.into_series())
        }, 
        &[col("BucketBCBS")], 
    GetOutput::from_type(DataType::Float64))
}
/// FX Curvature Delta, filtered by reporting ccy
pub fn fx_curv_delta (op: &OCP) -> Expr {
    risk_filtered_by_ccy(op, curv_delta_spot("FX") )
}
// FX CurvatureDelta Weighted, filtered by reporting ccy
pub fn fx_curv_delta_weighted (op: &OCP) -> Expr {
    risk_filtered_by_ccy(op, curv_delta_spot("FX") )*col("CurvatureRiskWeight")
}
/// FX PnL Up, filtered by reporting ccy
pub fn fx_pnl_up(op: &OCP) -> Expr {
    risk_filtered_by_ccy(op,rc_rcat_sens("Delta", "FX", col("PnL_Up")))
}
/// FX PnL Down, filtered by reporting ccy
pub fn fx_pnl_down(op: &OCP) -> Expr {
    risk_filtered_by_ccy(op,rc_rcat_sens("Delta", "FX", col("PnL_Down")))   
}


/// FX CVR Divide by 1.5
/// as per 21.98
/// column FxCurvDivEligibility must be present
fn fx_cvr_up_down(div: bool, risk: Expr) -> Expr {
    if !div{
        risk
    }else{
        risk
        .apply_many(|columns|{
            let mult: Vec<f64> = vec![1.; columns[0].len()];
            let mult = Float64Chunked::from_vec("multiplicator", mult);
            let mask = columns[1].bool()?;
            let mult = mult.set(mask, Some(1.5))?.into_series();
            columns[0].f64()?.divide(&mult)
        },
            &[col("FxCurvDivEligibility")], 
            GetOutput::from_type(DataType::Float64))
    }
}

pub fn fx_cvr_up(op: &OCP) -> Expr{
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false);
    let risk = risk_filtered_by_ccy(op,rc_cvr_spot("FX", CVR::Up));
    fx_cvr_up_down(div, risk)
} 
pub fn fx_cvr_down(op: &OCP) -> Expr{
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false);
    let risk = risk_filtered_by_ccy(op,rc_cvr_spot("FX", CVR::Down));
    fx_cvr_up_down(div, risk)
}

// Kb, Sb, KbPlus, KbMinus is same across all scenarios for АЧ
pub(crate) fn fx_curvature_kb_plus(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn fx_curvature_kb_minus(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn fx_curvature_kb(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn fx_curvature_sb(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

/// Calculate FX Curvature Capital charge
pub(crate) fn fx_curvature_charge_low(op: &OCP) -> Expr {
fx_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
pub(crate) fn fx_curvature_charge_medium(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
pub(crate) fn fx_curvature_charge_high(op: &OCP) -> Expr {
    fx_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn fx_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let ccy_regex = ccy_regex(op);
    
    let _suffix = scenario.as_str();
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false);

    let fx_curv_gamma = get_optional_parameter(op, format!("fx_curv_gamma{_suffix}").as_str(), &scenario.fx_curv_gamma);

    fx_curvature_charge(fx_curv_gamma,  rtrn, ccy_regex, div)
}

fn fx_curvature_charge(gamma: f64, return_metric: ReturnMetric, ccy_regex: String, div: bool) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rc"       => columns[0].clone(),
            "b"        => columns[1].clone(),
            "PnL_Up"   => columns[2].clone(),
            "PnL_Down" => columns[3].clone(),
            "SensitivitySpot" => columns[4].clone(),
            "CurvatureRiskWeight"=>columns[5].clone(),
            "FxCurvDivEligibility"=>columns[6].clone(),
        ]?;
        let ccy_regex = ccy_regex.clone();
        let df = df.lazy()
            .filter(
                col("rc").eq(lit("FX"))
                .and(col("PnL_Up").is_not_null().or(col("PnL_Down").is_not_null()))
                .and(col("b").apply(move |col|{
                    Ok( col
                    .utf8()?
                    .contains(&ccy_regex)?
                    .into_series() )
                },
                    GetOutput::from_type(DataType::Boolean)))
            )
            .groupby([col("b")])
            .agg([
                fx_cvr_up_down(div, cvr_up_spot()).sum().alias("cvr_up"),
                fx_cvr_up_down(div, cvr_down_spot()).sum().alias("cvr_down")
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;
        
        let res_len = columns[0].len();
        
        let kb_plus: Vec<f64> = kb_plus_minus_simple(&df["cvr_up"])?;
        match return_metric {
            ReturnMetric::KbPlus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_plus.iter().sum()).as_slice().unwrap())),
            _ => (), }

        let kb_minus: Vec<f64> = kb_plus_minus_simple(&df["cvr_down"])?;
        match return_metric {
            ReturnMetric::KbMinus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_minus.iter().sum()).as_slice().unwrap())),
            _ => (), }

        
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs_curvature(kb_plus, kb_minus,df["cvr_up"].f64()?.into_iter(),df["cvr_down"].f64()?.into_iter())?;
        match return_metric {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        // 325ag
        let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), gamma );
        let phi = phi(&sbs);
        gamma = gamma*phi;

        
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::Curvature)
    },
        &[
            col("RiskClass"),
            col("BucketBCBS"), 
            col("PnL_Up"),
            col("PnL_Down"),
            col("SensitivitySpot"),
            col("CurvatureRiskWeight"),
            col("FxCurvDivEligibility")
        ],
        GetOutput::from_type(DataType::Float64))
}
