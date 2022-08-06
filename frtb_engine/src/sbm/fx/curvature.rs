use crate::prelude::*;
use ndarray::{Array1, Array2};
use sbm::common::*;

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
    risk_filtered_by_ccy(op, curv_delta("FX") )
}
// FX CurvatureDelta Weighted, filtered by reporting ccy
pub fn fx_curv_delta_weighted (op: &OCP) -> Expr {
    risk_filtered_by_ccy(op, curv_delta("FX") )*col("CurvatureRiskWeight")
}
/// FX PnL Up, filtered by reporting ccy
pub fn fx_pnl_up(op: &OCP) -> Expr {
    risk_filtered_by_ccy(op,rc_rcat_sens("FX", "Delta", col("PnL_Up")))
}
/// FX PnL Down, filtered by reporting ccy
pub fn fx_pnl_down(op: &OCP) -> Expr {
    risk_filtered_by_ccy(op,rc_rcat_sens("FX", "Delta", col("PnL_Down")))   
}


// FX CVR Up
pub fn fx_cvr_up(op: &OCP) -> Expr {
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false);
    if !div{
        risk_filtered_by_ccy(op,rc_cvr("FX", CVR::Up))
    }else{
        risk_filtered_by_ccy(op,rc_cvr("FX", CVR::Up))
        .apply_many(|columns|{
            let res: Series = columns[0].f64()?
                .into_iter()
                .zip(columns[1].bool()?)
                .map(|(x,y)|{
                    let div_elegible = y.unwrap_or_default();
                    match x{
                        None => None,
                        Some(v) if div_elegible => Some(v/1.5),
                        Some(v) => Some(v),
                    }
                }).collect();
            Ok(res)
        },
            &[col("FxCurvDivEligibility")], 
            GetOutput::from_type(DataType::Float64))
    }
}
// FX CVR Down
pub fn fx_cvr_down(op: &OCP) -> Expr {
    let div = get_optional_parameter(op, "apply_fx_curv_div", &false);
    if !div{

    risk_filtered_by_ccy(op,rc_cvr("FX", CVR::Down))
    } else {
        risk_filtered_by_ccy(op,rc_cvr("FX", CVR::Down))
        .apply_many(|columns|{
            let res: Series = columns[0].f64()?
                .into_iter()
                .zip(columns[1].bool()?)
                .map(|(x,y)|{
                    let div_elegible = y.unwrap_or_default();
                    match x{
                        None => None,
                        Some(v) if div_elegible => Some(v/1.5),
                        Some(v) => Some(v),
                    }
                }).collect();
            Ok(res)
        },
            &[col("FxCurvDivEligibility")], 
            GetOutput::from_type(DataType::Float64))
    }
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
    let _suffix = scenario.as_str();

    let fx_curv_gamma = get_optional_parameter(op, format!("fx_curv_gamma{_suffix}").as_str(), &scenario.fx_curv_gamma);

    fx_curvature_charge(fx_curv_gamma,  rtrn, op)
}

fn fx_curvature_charge(gamma: f64, return_metric: ReturnMetric, op: &OCP) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "b"        => columns[0].clone(),
            "cvr_up"   => columns[1].clone(),
            "cvr_down" => columns[2].clone(),
        ]?;
        let df = df.lazy()
            .filter(col("cvr_up").is_not_null().or(col("cvr_down").is_not_null()))
            .groupby([col("b")])
            .agg([
                col("cvr_up").sum(),
                col("cvr_down").sum()
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;

        let res_len = columns[0].len();
        
        let kb_plus: Vec<f64> = kb_plus_minus(&df["cvr_up"])?;
        match return_metric {
            ReturnMetric::KbPlus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_plus.iter().sum()).as_slice().unwrap())),
            _ => (), }

        let kb_minus: Vec<f64> = kb_plus_minus(&df["cvr_down"])?;
        match return_metric {
            ReturnMetric::KbMinus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_minus.iter().sum()).as_slice().unwrap())),
            _ => (), }

        
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs_f(kb_plus, kb_minus,&df["cvr_up"],&df["cvr_down"])?;
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
        &[col("BucketBCBS"),
        fx_cvr_up(op),
        fx_cvr_down(op)],
        GetOutput::from_type(DataType::Float64))
}
