use base_engine::prelude::*;
use rayon::prelude::IntoParallelIterator;
use crate::sbm::common::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;

pub fn total_ir_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("Delta", "GIRR", total_delta_sens())
}
/// Helper functions
fn girr_delta_sens_weighted_spot() -> Expr {
    rc_tenor_weighted_sens( "Delta", "GIRR", "SensitivitySpot", "SensWeights",0)
}
fn girr_delta_sens_weighted_025y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_025Y", "SensWeights",1)
}
fn girr_delta_sens_weighted_05y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_05Y", "SensWeights",2)
}
fn girr_delta_sens_weighted_1y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_1Y", "SensWeights",3)
}
fn girr_delta_sens_weighted_2y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_2Y", "SensWeights",4)
}
fn girr_delta_sens_weighted_3y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_3Y", "SensWeights",5)
}
fn girr_delta_sens_weighted_5y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_5Y","SensWeights", 6)
}
fn girr_delta_sens_weighted_10y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_10Y","SensWeights", 7)
}
fn girr_delta_sens_weighted_15y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_15Y","SensWeights", 8)
}
fn girr_delta_sens_weighted_20y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_20Y","SensWeights", 9)
}
fn girr_delta_sens_weighted_30y() -> Expr {
    rc_tenor_weighted_sens("Delta","GIRR", "Sensitivity_30Y","SensWeights", 10)
}

/// Total GIRR Delta Seins
pub(crate) fn girr_delta_sens_weighted(_: &OCP) -> Expr {
    girr_delta_sens_weighted_spot().fill_null(0.)
    + girr_delta_sens_weighted_025y().fill_null(0.)
    + girr_delta_sens_weighted_05y().fill_null(0.)
    + girr_delta_sens_weighted_1y().fill_null(0.)
    + girr_delta_sens_weighted_2y().fill_null(0.)
    + girr_delta_sens_weighted_3y().fill_null(0.)
    + girr_delta_sens_weighted_5y().fill_null(0.)
    + girr_delta_sens_weighted_10y().fill_null(0.)
    + girr_delta_sens_weighted_15y().fill_null(0.)
    + girr_delta_sens_weighted_20y().fill_null(0.)
    + girr_delta_sens_weighted_30y().fill_null(0.)
}

/// Interm Result: GIRR Delta Sb <--> Sb Low == Sb Medium == Sb High
pub(crate) fn girr_delta_sb(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Sb)  
}

///calculate GIRR Delta Low Capital charge
pub(crate) fn girr_delta_charge_low(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
/// Interm Result: GIRR Delta Kb Low
pub(crate) fn girr_delta_kb_low(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*LOW_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate GIRR Delta Medium Capital charge
pub(crate) fn girr_delta_charge_medium(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::CapitalCharge)  
}
/// Interm Result: GIRR Delta Kb Medium
pub(crate) fn girr_delta_kb_medium(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO, ReturnMetric::Kb)  
}

///calculate GIRR Delta High Capital charge
pub(crate) fn girr_delta_charge_high(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::CapitalCharge)
}
/// Interm Result: GIRR Delta Kb High
pub(crate) fn girr_delta_kb_high(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO, ReturnMetric::Kb)  
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig, rtrn: ReturnMetric) -> Expr {
    let juri: Jurisdiction = get_jurisdiction(op);
    let _suffix = scenario.as_str();

    // Take MEDIUM scenario here because scenario_fn is to be applied post factum
    let girr_delta_rho_same_curve = get_optional_parameter_array(op, format!("girr_delta_rho_same_curve").as_str(),&MEDIUM_CORR_SCENARIO.girr_delta_rho_same_curve);
    let girr_delta_rho_diff_curve = get_optional_parameter(op, format!("girr_delta_rho_diff_curve").as_str(), &MEDIUM_CORR_SCENARIO.girr_delta_rho_diff_curve);
    let girr_delta_rho_infl = get_optional_parameter(op,format!("girr_delta_rho_infl").as_str(),&MEDIUM_CORR_SCENARIO.girr_delta_rho_infl);
    let girr_delta_rho_xccy = get_optional_parameter(op,format!("girr_delta_rho_xccy").as_str(), &MEDIUM_CORR_SCENARIO.girr_delta_rho_xccy);

    let girr_delta_gamma = get_optional_parameter(op, format!("girr_delta_gamma{_suffix}").as_str(), &scenario.girr_gamma);
    let girr_delta_gamma_crr2_erm2 = get_optional_parameter(op, format!("girr_delta_gamma_erm2{_suffix}").as_str(), &scenario.girr_gamma_crr2_erm2);
    let erm2ccys =  get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_crr2);

    girr_delta_charge(girr_delta_gamma, girr_delta_rho_same_curve, girr_delta_rho_diff_curve, girr_delta_rho_infl, girr_delta_rho_xccy, rtrn, juri, girr_delta_gamma_crr2_erm2, erm2ccys, scenario.scenario_fn)
}

fn girr_delta_charge<F>(girr_delta_gamma: f64, girr_delta_rho_same_curve: Array2<f64>, 
    girr_delta_rho_diff_curve: f64, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,
    return_metric: ReturnMetric, juri: Jurisdiction, _erm2_gamma: f64,
    _erm2ccys: Vec<String>, scenario_fn: F) -> Expr
    where F: Fn(f64) -> f64 + Sync + Send + Copy + 'static,{

    apply_multiple( move |columns| {
        
        let df = df![
            "rcat" => columns[15].clone(),
            "rc" =>   columns[0].clone(), 
            "rf" =>   columns[1].clone(),
            "rft" =>  columns[2].clone(),
            "b" =>    columns[3].clone(),
            "y0" =>   columns[4].clone(),
            "y025" => columns[5].clone(),
            "y05" =>  columns[6].clone(),
            "y1" =>   columns[7].clone(),
            "y2" =>   columns[8].clone(),
            "y3" =>   columns[9].clone(),
            "y5" =>   columns[10].clone(),
            "y10" =>  columns[11].clone(),
            "y15" =>  columns[12].clone(),
            "y20" =>  columns[13].clone(),
            "y30" =>  columns[14].clone(),

            "w0" =>   columns[16].clone(),
            "w025" => columns[17].clone(),
            "w05" =>  columns[18].clone(),
            "w1" =>   columns[19].clone(),
            "w2" =>   columns[20].clone(),
            "w3" =>   columns[21].clone(),
            "w5" =>   columns[22].clone(),
            "w10" =>  columns[23].clone(),
            "w15" =>  columns[24].clone(),
            "w20" =>  columns[25].clone(),
            "w30" =>  columns[26].clone(),
        ]?;
        let df = df.lazy()
            .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Delta"))))
            .groupby([col("b"), col("rf"), col("rft")])
            .agg([
                (col("y0")*col("w0")).sum(),
                (col("y025")*col("w025")).sum(),
                (col("y05")*col("w05")).sum(),
                (col("y1")*col("w1")).sum(),
                (col("y2")*col("w2")).sum(),
                (col("y3")*col("w3")).sum(),
                (col("y5")*col("w5")).sum(),
                (col("y10")*col("w10")).sum(),
                (col("y15")*col("w15")).sum(),
                (col("y20")*col("w20")).sum(),
                (col("y30")*col("w30")).sum()            
            ])
            .fill_null(lit::<f64>(0.))
            .with_columns([
                when(col("rft").eq(lit("XCCY")))
                .then(col("y0"))
                .otherwise(NULL.lit())
                .alias("XCCY"),
                when(col("rft").eq(lit("Inflation")))
                .then(col("y0"))
                .otherwise(NULL.lit())
                .alias("Inflation"),
            ])
            .select([col("*").exclude(&["rft"])])
            .collect()?;
        
        let part = df.partition_by(["b"])?;

        let res_buckets_kbs_sbs: Result<Vec<((String,f64),f64)>> = part
        .into_par_iter()
        .map(|bdf|{
            bucket_kb_sb_single_type(&bdf, 
                &girr_delta_rho_same_curve, girr_delta_rho_diff_curve,
                 scenario_fn,
                &["y025", "y05", "y1", "y2", "y3", "y5", "y10", "y15", "y20", "y30"],
                Some((girr_delta_rho_infl, girr_delta_rho_xccy)),
                None)
        })
        .collect();

        let buckets_kbs_sbs = res_buckets_kbs_sbs?;
        let (buckets_kbs, sbs): (Vec<(String, f64)>, Vec<f64>) = buckets_kbs_sbs.into_iter().unzip();
        let (_buckets, kbs): (Vec<String>, Vec<f64>) = buckets_kbs.into_iter().unzip();

        // Early return Kb or Sb is that is the required metric
        let res_len = columns[0].len();
        match return_metric {
            ReturnMetric::Kb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, kbs.iter().sum()).as_slice().unwrap())),
            ReturnMetric::Sb => return Ok( Series::new("res", Array1::<f64>::from_elem(res_len, sbs.iter().sum()).as_slice().unwrap())),
            _ => (),
        }
        // Need to differentiate between CRR2 and BCBS
        // 325ag
        let mut gamma = match juri {
            #[cfg(feature = "CRR2")]
            Jurisdiction::CRR2 => { 
                let _buckets_str: Vec<&str> = _buckets.iter().map(|s| &**s).collect();
                build_girr_crr2_gamma(&_buckets_str, &_erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                girr_delta_gamma, _erm2_gamma ) },
            _ => Array2::from_elem((kbs.len(), kbs.len()), girr_delta_gamma ),
            };

        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), col("BucketBCBS"), 
    col("SensitivitySpot"),
    col("Sensitivity_025Y"),
    col("Sensitivity_05Y"),
    col("Sensitivity_1Y"),
    col("Sensitivity_2Y"),
    col("Sensitivity_3Y"),
    col("Sensitivity_5Y"),
    col("Sensitivity_10Y"),
    col("Sensitivity_15Y"),
    col("Sensitivity_20Y"),
    col("Sensitivity_30Y"),
    col("RiskCategory"),
    //col("SensWeights"),
    col("SensWeights").arr().get(0),
    col("SensWeights").arr().get(1),
    col("SensWeights").arr().get(2),
    col("SensWeights").arr().get(3),
    col("SensWeights").arr().get(4),
    col("SensWeights").arr().get(5),
    col("SensWeights").arr().get(6),
    col("SensWeights").arr().get(7),
    col("SensWeights").arr().get(8),
    col("SensWeights").arr().get(9), 
    col("SensWeights").arr().get(10),
    ], 
        GetOutput::from_type(DataType::Float64))
}

/// 325ag
/// TODO test
#[cfg(feature = "CRR2")]
pub(crate) fn build_girr_crr2_gamma(buckets: &[&str], erm2ccys: &[&str], base_gamma: f64, erma2vseur: f64) -> Array2<f64>
//where I: Iterator<Item = S>,
{
    let mut gamma = Array2::from_elem((buckets.len(), buckets.len()), base_gamma );

    gamma.axis_iter_mut(Axis(0)) // Iterate over rows
    .enumerate()
    .for_each(|(i, mut row)|{
        let buck1 = unsafe{buckets.get_unchecked(i)};
        if erm2ccys.contains(buck1) | (*buck1 == "EUR") { // if a row is ERM2 or EUR
            row.indexed_iter_mut().for_each(|(j, x)|{
                let buck2 = unsafe{buckets.get_unchecked(j)};
                if (*buck1 == "EUR")&erm2ccys.contains(buck2){ // if row is EUR and col is ERM2
                    *x = erma2vseur;
                } else if erm2ccys.contains(buck1)&(*buck2 == "EUR") { // if row is ERM2 and col is EUR
                    *x = erma2vseur;
                }
            })
        }
    });
    gamma
}

/// 21.46 GIRR delta risk correlation within same bucket, same curve
/// results in 10x10 matrix
/// Used at the initiation of the program using OnceCell
pub(crate) fn girr_corr_matrix() -> Array2<f64> {
    let theta: f64 = -0.03;
    let mut base_weights = Array2::<f64>::zeros((10, 10));
    let tenors = [ 0.25, 0.5, 1., 2., 3., 5., 10., 15., 20., 30. ];

    for ((row, col), val) in base_weights.indexed_iter_mut() {
        let tr = tenors[row];
        let tc = tenors[col];
        *val = f64::max( f64::exp( theta * f64::abs(tr-tc) / tr.min(tc) ) , 0.4 );
    }
    base_weights
}
/// Exporting Measures
pub(crate) fn girr_delta_measures()-> Vec<Measure<'static>> {
    vec![
        Measure{
            name: "GIRR_DeltaSens".to_string(),
            calculator: Box::new(total_ir_delta_sens),
            aggregation: None,
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaSens_Weighted".to_string(),
            calculator: Box::new(girr_delta_sens_weighted),
            aggregation: None,
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaSb".to_string(),
            calculator: Box::new(girr_delta_sb),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaCharge_Low".to_string(),
            calculator: Box::new(girr_delta_charge_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaKb_Low".to_string(),
            calculator: Box::new(girr_delta_kb_low),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaCharge_Medium".to_string(),
            calculator: Box::new(girr_delta_charge_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaKb_Medium".to_string(),
            calculator: Box::new(girr_delta_kb_medium),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaCharge_High".to_string(),
            calculator: Box::new(girr_delta_charge_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },

        Measure{
            name: "GIRR_DeltaKb_High".to_string(),
            calculator: Box::new(girr_delta_kb_high),
            aggregation: Some("first"),
            precomputefilter: Some(col("RiskCategory").eq(lit("Delta")).and(col("RiskClass").eq(lit("GIRR"))))
        },
    ]
}