use crate::{prelude::*, sbm::common::{cvr_up, cvr_down, CVR, rc_cvr, rc_rcat_sens, across_bucket_agg, SBMChargeType, phi}};

use base_engine::prelude::OCP;
use polars::prelude::*;

use crate::{sbm::common::curv_delta, helpers::ReturnMetric};
#[cfg(feature = "CRR2")]
use super::delta::build_girr_crr2_gamma;

pub fn ir_curv_delta (_: &OCP) -> Expr {
    curv_delta("GIRR")
}

/// Helper functions
pub fn girr_curv_delta_weighted(op: &OCP) -> Expr {
    ir_curv_delta(op)*col("CurvatureRiskWeight")
}

pub fn girr_cvr_down(_: &OCP) -> Expr {
    rc_cvr("GIRR", CVR::Down)
}

pub fn girr_cvr_up(_: &OCP) -> Expr {
    rc_cvr("GIRR", CVR::Up)
}

pub fn girr_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("GIRR", "Delta", col("PnL_Up"))
}

pub fn girr_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("GIRR", "Delta", col("PnL_Down"))    
}

// Kb, Sb, KbPlus, KbMinus is same across all scenarios for GIRR
pub(crate) fn girr_curvature_kb_plus(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn girr_curvature_kb_minus(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn girr_curvature_kb(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn girr_curvature_sb(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

/// Calculate GIRR Curvature Capital charge
pub(crate) fn girr_curvature_charge_low(op: &OCP) -> Expr {
girr_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
pub(crate) fn girr_curvature_charge_medium(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
pub(crate) fn girr_curvature_charge_high(op: &OCP) -> Expr {
    girr_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    let girr_curv_gamma = get_optional_parameter(op, format!("girr_curv_gamma{_suffix}").as_str(), &scenario.girr_curv_gamma);
    let girr_vega_gamma_crr2_erm2 = get_optional_parameter(op, format!("girr_curv_gamma_crr2_erm2{_suffix}").as_str(), &scenario.girr_curv_gamma_crr2_erm2);
    let erm2ccys =  get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_crr2);


    girr_curvature_charge(girr_curv_gamma,girr_vega_gamma_crr2_erm2,  rtrn, juri, erm2ccys)
}

/// https://www.clarusft.com/frtb-curvature-risk-charge/
/// Note: single Curvature Risk Charge (in Rates), ie CVR up/down, per currency
/// We therefore simply sum (no rho) CVR_Up/CVR_Down within a bucket
fn girr_curvature_charge(girr_curv_gamma: f64, erm2_gamma: f64,
    return_metric: ReturnMetric, juri: Jurisdiction, erm2ccys: Vec<String>) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rc"       => columns[0].clone(),
            "b"        => columns[1].clone(),
            "cvr_up"   => columns[2].clone(),
            "cvr_down" => columns[3].clone(),
        ]?;
        let df = df.lazy()
            .filter(col("rc").eq(lit("GIRR")).and(col("cvr_up").is_not_null().or(col("cvr_down").is_not_null())))
            .groupby([col("b")])
            .agg([
                col("cvr_up").sum(),
                col("cvr_down").sum()
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;

        let res_len = columns[0].len();
        
        let kb_plus: Vec<f64> = df["cvr_up"]
            .f64()?
            .into_iter()
            .map(|cv_up|
                f64::max(cv_up.unwrap_or_else(||0.), 0.)
            )
            .collect();

        match return_metric {
            ReturnMetric::KbPlus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_plus.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        let kb_minus: Vec<f64> = df["cvr_down"]
            .f64()?
            .into_iter()
            .map(|cv_down|
                f64::max(cv_down.unwrap_or_else(||0.), 0.)
            )
            .collect();

        match return_metric {
            ReturnMetric::KbMinus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_minus.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        let kbs_sbs: Vec<(f64, f64)> = kb_plus.into_iter()
            .zip(kb_minus.into_iter())
            .zip(df["cvr_up"].f64()?.into_iter())
            .zip(df["cvr_down"].f64()?.into_iter())
            .map(|(((kb_p, kb_m), cv_up), cv_down)|
            if kb_p>kb_m{
                (kb_p, cv_up.unwrap_or_else(||0.))
            } else if kb_m>kb_p {
                (kb_m, cv_down.unwrap_or_else(||0.))
            } else { // 21.5.3.a.iii
                if cv_up>cv_down{
                    (kb_p, cv_up.unwrap_or_else(||0.))
                } else {
                    (kb_m, cv_down.unwrap_or_else(||0.))
                }
            }
        )
        .collect::<Vec<(f64, f64)>>();
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        match return_metric {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        let buckets: Vec<&str> = df["b"].utf8()?.into_iter().map(|s| s.unwrap_or_else(||"Default")).collect();

        // 325ag
        let mut gamma = match juri {
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => { build_girr_crr2_gamma(&buckets, &erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                girr_curv_gamma, erm2_gamma ) },
            _ => Array2::from_elem((kbs.len(), kbs.len()), girr_curv_gamma ),
        };

        let phi = phi(&sbs);
        gamma = gamma*phi;

        
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::Curvature)
    },
        &[col("RiskClass"), col("BucketBCBS"),
        cvr_up(),
        cvr_down()],
        GetOutput::from_type(DataType::Float64))
}