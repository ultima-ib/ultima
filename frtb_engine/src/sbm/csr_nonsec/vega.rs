use base_engine::prelude::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;

pub fn total_csrnonsec_vega_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Vega", "CSR_nonSec", total_vega_curv_sens())
}

pub fn total_csrnonsec_vega_sens_weighted_bcbs (op: &OCP) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    
    match juri{
        #[cfg(feature = "CRR2")]
        Jurisdiction::CRR2 =>total_csrnonsec_vega_sens(op)*col("SensWeightsCRR2").arr().get(0),
        Jurisdiction::BCBS =>total_csrnonsec_vega_sens(op)*col("SensWeights").arr().get(0)
    }
}

///calculate CSR Non Sec Interm Result
pub(crate) fn csr_nonsec_vega_sb(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Sb)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_low(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_low(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_medium(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_medium(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

///Interm Result
pub(crate) fn csr_nonsec_vega_kb_high(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate CSR Non Sec Vega Low Capital charge
pub(crate) fn csr_nonsec_vega_charge_high(op: &OCP) -> Expr {
    csr_nonsec_vega_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn csr_nonsec_vega_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    let (weight, bucket_col, name_rho_vec,
        rho_opt, 
        gamma,
        special_bucket) =
        match juri{
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => (
            col("SensWeightsCRR2").arr().get(0),
            col("BucketCRR2"),
            Vec::from(scenario.base_csr_nonsec_rho_name_crr2),
            &scenario.base_vega_rho,
            &scenario.csr_nonsec_gamma_crr2,
            Some("18")
            ),

            Jurisdiction::BCBS=>
            (
            col("SensWeights").arr().get(0),
            col("BucketBCBS"),
            Vec::from(scenario.base_csr_nonsec_rho_name_bcbs),
            &scenario.base_vega_rho,
            &scenario.csr_nonsec_gamma,
            Some("16")
            )
        };

    let csr_gamma = get_optional_parameter_array(op, format!("csr_vega_gamma{_suffix}").as_str(), gamma);
    let base_csr_rho_bucket = get_optional_parameter_vec(op, format!("csr_rho_diff_name_bucket{_suffix}").as_str(), &name_rho_vec);
    let csr_vega_rho = get_optional_parameter_array(op, format!("csr_opt_mat_vega_rho{_suffix}").as_str(), rho_opt);

    csr_nonsec_vega_charge(weight, bucket_col, &scenario.scenario_fn, 
        csr_vega_rho, base_csr_rho_bucket, 
        csr_gamma, special_bucket, "CSR_nonSec", "Vega", rtrn)
}

/// Used by CSR nonSec, CSR secCTP Vegas
pub(crate) fn csr_nonsec_vega_charge<F>(
    weight: Expr,
    bucket_col: Expr, scenario_fn: F, 
    opt_mat_rho: Array2<f64>, rho_diff_curve: Vec<f64>,
    gamma: Array2<f64>,
    special_bucket: Option<&'static str>, risk_class: &'static str, risk_cat: &'static str,
    rtrn: ReturnMetric) -> Expr
where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static, {

    apply_multiple( move |columns| {
        let df = df![
            "rc" =>   columns[0].clone(), 
            "rf" =>   columns[1].clone(),
            "b" =>    columns[2].clone(),
            "y05" =>  columns[3].clone(),
            "y1" =>   columns[4].clone(),
            "y3" =>   columns[5].clone(),
            "y5" =>   columns[6].clone(),
            "y10" =>  columns[7].clone(),
            "w" =>    columns[8].clone(),
            "rcat" => columns[9].clone(),
        ]?;        
        
        // concat_lst is actually slower than 
        let df = df.lazy()
            .filter(col("rc").eq(lit(risk_class))
                .and(col("rcat").eq(lit(risk_cat))))
            .groupby([col("b"), col("rf")])
            .agg([
                (col("y05")*col("w")).sum(),    
                (col("y1")*col("w")).sum(), 
                (col("y3")*col("w")).sum(), 
                (col("y5")*col("w")).sum(), 
                (col("y10")*col("w")).sum(),    
            ])
            //.fill_null(lit::<f64>(0.))
            .collect()?;
        
        if df.height() == 0 { return Ok( Series::from_vec("res", vec![0.; columns[0].len() ] as Vec<f64>) )};

        let kbs_sbs = all_kbs_sbs_single_type(df, 
            &opt_mat_rho,
            &rho_diff_curve, 
            scenario_fn, 
            &vec!["y05", "y1", "y3", "y5", "y10"],
            special_bucket            
        )?; 

        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();

        let res_len = columns[0].len();
        match rtrn {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    
    &[ col("RiskClass"), col("RiskFactor"),
     bucket_col, 
    //y05, y1, y3, y5, y10,
    col("Sensitivity_05Y"),
    col("Sensitivity_1Y"),
    col("Sensitivity_3Y"),
    col("Sensitivity_5Y"),
    col("Sensitivity_10Y"),
    weight,// risk weight
     col("RiskCategory")], 
    GetOutput::from_type(DataType::Float64))
}

/// Exporting Measures
pub(crate) fn csrnonsec_vega_measures()-> Vec<Measure<'static>> {
    vec![
        Measure{
            name: "CSR_nonSec_VegaSens".to_string(),
            calculator: Box::new(total_csrnonsec_vega_sens),
            aggregation: None,
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaSens_Weighted".to_string(),
            calculator: Box::new(total_csrnonsec_vega_sens_weighted_bcbs),
            aggregation: None,
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaSb".to_string(),
            calculator: Box::new(csr_nonsec_vega_sb),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },
        
        Measure{
            name: "CSR_nonSec_VegaCharge_Low".to_string(),
            calculator: Box::new(csr_nonsec_vega_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaKb_Low".to_string(),
            calculator: Box::new(csr_nonsec_vega_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaCharge_Medium".to_string(),
            calculator: Box::new(csr_nonsec_vega_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaKb_Medium".to_string(),
            calculator: Box::new(csr_nonsec_vega_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaCharge_High".to_string(),
            calculator: Box::new(csr_nonsec_vega_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },

        Measure{
            name: "CSR_nonSec_VegaKb_High".to_string(),
            calculator: Box::new(csr_nonsec_vega_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Vega")).and(col("RiskClass").eq(lit("CSR_nonSec"))))
        },
    ]
}


