use base_engine::prelude::OCP;
use ndarray::{Array2, Array1};
use crate::prelude::*;
//use ndarray::{Array1, Array2};
use polars::prelude::*;

pub fn csrnonsec_curv_delta (_: &OCP) -> Expr {
    curv_delta_5("CSR_nonSec")
}
/// Helper functions
pub fn csrnonsec_curv_delta_weighted(op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 =>csrnonsec_curv_delta(op)*col("CurvatureRiskWeightCRR2"),
        Jurisdiction::BCBS =>csrnonsec_curv_delta(op)*col("CurvatureRiskWeight"),
    }
}

pub fn csrnonsec_cvr_down(_: &OCP) -> Expr {
    rc_cvr_5("CSR_nonSec", CVR::Down)
}
pub fn csrnonsec_cvr_up(_: &OCP) -> Expr {
    rc_cvr_5("CSR_nonSec", CVR::Up)
}
pub fn csrnonsec_pnl_up(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_nonSec", col("PnL_Up"))
}
pub fn csrnonsec_pnl_down(_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "CSR_nonSec", col("PnL_Down"))    
}

pub(crate) fn csrnonsec_curvature_kb_plus_low(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrnonsec_curvature_kb_minus_low(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrnonsec_curvature_kb_low(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrnonsec_curvature_sb_low(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrnonsec_curvature_charge_low(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

pub(crate) fn csrnonsec_curvature_kb_plus_medium(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrnonsec_curvature_kb_minus_medium(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrnonsec_curvature_kb_medium(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrnonsec_curvature_sb_medium(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrnonsec_curvature_charge_medium(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

pub(crate) fn csrnonsec_curvature_kb_plus_high(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbPlus)  
}
pub(crate) fn csrnonsec_curvature_kb_minus_high(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::KbMinus)  
}
pub(crate) fn csrnonsec_curvature_kb_high(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}
pub(crate) fn csrnonsec_curvature_sb_high(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Sb)  
}
pub(crate) fn csrnonsec_curvature_charge_high(op: &OCP) -> Expr {
    csrnonsec_curvature_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}

fn csrnonsec_curvature_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let _suffix = scenario.as_str();
    let juri: Jurisdiction = get_jurisdiction(op);

    let (weight, bucket_col, name_rho_vec,
        gamma,
        special_bucket) =
        match juri{
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (
            col("CurvatureRiskWeightCRR2"),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_nonsec_rho_name_crr2_curv),
            &scenario.csr_nonsec_gamma_crr2_curv,
            Some(18)
            ),

            Jurisdiction::BCBS=>
            (
            col("CurvatureRiskWeight"),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_nonsec_rho_name_bcbs_curv),
            &scenario.csr_nonsec_gamma_curv,
            Some(16)
            )
        };

    let csr_nonsec_curv_gamma = get_optional_parameter_array(op, format!("csr_nonsec_curv_gamma{_suffix}").as_str(), &gamma);
    let csr_nonsec_curv_rho = get_optional_parameter_vec(op, format!("csr_nonsec_curv_rho{_suffix}").as_str(), &name_rho_vec);
    csrnonsec_curvature_charge(csr_nonsec_curv_rho, csr_nonsec_curv_gamma,
          rtrn, special_bucket, weight, bucket_col, "CSR_nonSec")
}

pub (crate) fn csrnonsec_curvature_charge(csr_curv_rho: Vec<f64>, csr_curv_gamma: Array2<f64>, return_metric: ReturnMetric, 
special_bucket: Option<usize>, weight: Expr, bucket_col: Expr, rc: &'static str) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rc"       => columns[0].clone(),
            "b"        => columns[1].clone(),
            "rf"       => columns[2].clone(),
            "PnL_Up"   => columns[3].clone(),
            "PnL_Down" => columns[4].clone(),
            "Sensitivity_05Y"           => columns[5].clone(),
            "Sensitivity_1Y"            => columns[6].clone(),
            "Sensitivity_3Y"            => columns[7].clone(),
            "Sensitivity_5Y"            => columns[8].clone(),
            "Sensitivity_10Y"           => columns[9].clone(),
            "CurvatureRiskWeight"       => columns[10].clone(),
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit(rc)).and(col("PnL_Up").is_not_null().or(col("PnL_Down").is_not_null())))
            .groupby([col("b"), col("rf")])
            .agg([
                cvr_up_5().sum().alias("cvr_up"),
                cvr_down_5().sum().alias("cvr_down"),
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;

        let res_len = columns[0].len();
        let (kb_plus_cvr_up, kb_minus_cvr_down): (Vec<(f64, f64)>, Vec<(f64, f64)>) = curvature_kb_plus_minus(
            df, 
            &csr_curv_rho,
            special_bucket,
        )?;
        let (kb_plus, cvr_up): (Vec<f64>, Vec<f64>) = kb_plus_cvr_up.into_iter().unzip();
        let (kb_minus, cvr_down): (Vec<f64>, Vec<f64>) = kb_minus_cvr_down.into_iter().unzip();
    
        match return_metric {
            ReturnMetric::KbPlus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_plus.iter().sum()).as_slice().unwrap())),
            ReturnMetric::KbMinus => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kb_minus.iter().sum()).as_slice().unwrap())),
            _ => (), }
        
        let cvr_up_opt: Vec<Option<f64>> = cvr_up.into_iter().map(|x|Some(x)).collect();
        let cvr_down_opt: Vec<Option<f64>> = cvr_down.into_iter().map(|x|Some(x)).collect();
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs_curvature(kb_plus, kb_minus, cvr_up_opt.into_iter(), cvr_down_opt.into_iter())?;
        match return_metric {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        let phi = phi(&sbs);
        let gamma = phi*csr_curv_gamma.view();

        across_bucket_agg(kbs, sbs, &gamma, res_len, SBMChargeType::Curvature)
    },
        &[col("RiskClass"),
        bucket_col,
        col("RiskFactor"),
        col("PnL_Up"),
        col("PnL_Down"),
        col("Sensitivity_05Y"),
        col("Sensitivity_1Y"),
        col("Sensitivity_3Y"),
        col("Sensitivity_5Y"),
        col("Sensitivity_10Y"),
        weight],
        GetOutput::from_type(DataType::Float64))
}