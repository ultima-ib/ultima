use base_engine::prelude::*;
use ndarray::Order;
use rayon::iter::{ParallelBridge, IntoParallelRefIterator};
use crate::sbm::common::*;
use crate::prelude::*;
use std::mem::MaybeUninit;

use polars::prelude::*;
use ndarray::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;

pub fn total_ir_delta_sens (_: &OCP) -> Expr {
    rc_rcat_sens("GIRR", "Delta", total_delta_sens())
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

    let girr_delta_rho_same_curve = get_optional_parameter_array(op, format!("girr_delta_rho_same_curve{_suffix}").as_str(),&scenario.girr_delta_rho_same_curve);
    let girr_delta_rho_diff_curve = get_optional_parameter_array(op, format!("girr_delta_rho_diff_curve{_suffix}").as_str(), &scenario.girr_delta_rho_diff_curve);
    let girr_delta_rho_infl = get_optional_parameter(op,format!("girr_delta_rho_infl{_suffix}").as_str(),&scenario.girr_delta_rho_infl);
    let girr_delta_rho_xccy = get_optional_parameter(op,format!("girr_delta_rho_xccy{_suffix}").as_str(), &scenario.girr_delta_rho_xccy);

    let girr_delta_gamma = get_optional_parameter(op, format!("girr_delta_gamma{_suffix}").as_str(), &scenario.girr_gamma);
    let girr_delta_gamma_crr2_erm2 = get_optional_parameter(op, format!("girr_delta_gamma_erm2{_suffix}").as_str(), &scenario.girr_gamma_crr2_erm2);
    let erm2ccys =  get_optional_parameter_vec(op, "erm2_ccys", &scenario.erm2_crr2);

    girr_delta_charge(girr_delta_gamma, girr_delta_rho_same_curve, girr_delta_rho_diff_curve, girr_delta_rho_infl, girr_delta_rho_xccy, rtrn, juri, girr_delta_gamma_crr2_erm2, erm2ccys)
}

/// References moved into apply_multiple have to be 'static. Hence we can't pass a ref to a newly created Array2
/// The only solution is to clone Array2 and make this function take ownership of it
/// This allows for flexibility with overriding ScenarioConfig, but the drawback is cloning
/// girr_delta_charge is called up to 3 times(1 for each scenario) FOR EACH group in outer groupby(say N) per request
/// in total 3xN clones. Performance of such many clones has to be carefully accessed
fn girr_delta_charge(girr_delta_gamma: f64, girr_delta_rho_same_curve: Array2<f64>, 
    girr_delta_rho_diff_curve: Array2<f64>, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,
    return_metric: ReturnMetric, juri: Jurisdiction, erm2_gamma: f64,
    erm2ccys: Vec<String>) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
            "rcat" =>   columns[15].clone(),
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
        ]?;

        let df = df.lazy()
            .filter(col("rc").eq(lit("GIRR")).and(col("rcat").eq(lit("Delta"))))
            .groupby([col("b"), col("rf"), col("rft")])
            .agg([
                col("y0").sum(),
                col("y025").sum(),
                col("y05").sum(),
                col("y1").sum(),
                col("y2").sum(),
                col("y3").sum(),
                col("y5").sum(),
                col("y10").sum(),
                col("y15").sum(),
                col("y20").sum(),
                col("y30").sum()            
            ])
            .fill_null(lit::<f64>(0.))
            .collect()?;

        let part = df.partition_by(["b"])?;
        let res_buckets_kbs_sbs: Result<Vec<((&str,f64),f64)>> = part
        .par_iter()
        .map(|bdf|{
            girr_bucket_kb_sb(bdf, 
                &girr_delta_rho_same_curve, &girr_delta_rho_diff_curve, 
                girr_delta_rho_infl, girr_delta_rho_xccy)
        })
        .collect();

        let buckets_kbs_sbs = res_buckets_kbs_sbs?;
        let (buckets_kbs, sbs): (Vec<(&str, f64)>, Vec<f64>) = buckets_kbs_sbs.into_iter().unzip();
        let (buckets, kbs): (Vec<&str>, Vec<f64>) = buckets_kbs.into_iter().unzip();

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
            Jurisdiction::CRR2 => { build_girr_crr2_gamma(&buckets, &erm2ccys.iter().map(|s| &**s).collect::<Vec<&str>>(),
                girr_delta_gamma, erm2_gamma ) },
            _ => Array2::from_elem((kbs.len(), kbs.len()), girr_delta_gamma ),
            };

        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len(), SBMChargeType::DeltaVega)
    }, 
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), col("BucketBCBS"), 
    col("SensitivitySpot")*col("SensWeights").arr().get(0),
    col("Sensitivity_025Y")*col("SensWeights").arr().get(1),
    col("Sensitivity_05Y")*col("SensWeights").arr().get(2),
    col("Sensitivity_1Y")*col("SensWeights").arr().get(3),
    col("Sensitivity_2Y")*col("SensWeights").arr().get(4),
    col("Sensitivity_3Y")*col("SensWeights").arr().get(5),
    col("Sensitivity_5Y")*col("SensWeights").arr().get(6),
    col("Sensitivity_10Y")*col("SensWeights").arr().get(7),
    col("Sensitivity_15Y")*col("SensWeights").arr().get(8),
    col("Sensitivity_20Y")*col("SensWeights").arr().get(9), 
    col("Sensitivity_30Y")*col("SensWeights").arr().get(10),
    col("RiskCategory")], 
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

fn build_girr_corr_matrix( girr_delta_rho_same_curve: &Array2<f64>, 
    girr_delta_rho_diff_curve: &Array2<f64>, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,
    y: usize, i: usize, x: usize) -> Result<Array2<f64>> {

    let n = 10; //represents number of tenors (aka columns) of yield curve
    let y_curves = y / n ; //represents number of yield curves
    //let i_curves = i;
    //let x_curves = x;
    let total_len = y + i + x;
    let mut res = Array2::ones((0, 0));
    if total_len>0{
        let mut arr = Array2::<f64>::uninit((total_len, total_len));
        if y > 0 {
            let mut yield_slice = arr.slice_mut(s![..y, ..y]);
            yield_slice
            .exact_chunks_mut((n, n))
            .into_iter()
            .enumerate()
            .par_bridge()
            .for_each(|(i, chunk)|{
                let row_id = i/y_curves; //eg 27usize/10usize = 2usize
                let col_id = i%y_curves; //eg 27usize % 10usize = 7usize
                //21.46 GIRR delta risk correlation within same bucket, same curve
                if row_id == col_id {
                    //girr_delta_rho_same_curve.assign_to(chunk);
                    girr_delta_rho_same_curve.to_owned().move_into_uninit(chunk)
                //21.47 GIRR delta risk correlation within same bucket, different curves
                } else {
                    //girr_delta_rho_diff_curve.assign_to(chunk);
                    girr_delta_rho_diff_curve.to_owned().move_into_uninit(chunk)
                }
            });
        };
        if i>0 {
            // Can't use multi slice because slices intersect
            // Have to take advantage of Rust's non-lexical lifetimes
            let infl_rho_element = MaybeUninit::new(girr_delta_rho_infl);
            let mut inflation_slice_row = arr.slice_mut(s![y..y+i, ..]);
            inflation_slice_row.fill(infl_rho_element);
            let mut inflation_slice_col = arr.slice_mut(s![..,y..y+i]);
            inflation_slice_col.fill(infl_rho_element);
        }
        if x>0 {
            let xccy_rho_elem = MaybeUninit::new(girr_delta_rho_xccy);
            let mut xccy_slice_row = arr.slice_mut(s![y+i.., ..]);
            xccy_slice_row.fill(xccy_rho_elem);
            let mut xccy_slice_col = arr.slice_mut(s![.., y+i..]);
            xccy_slice_col.fill(xccy_rho_elem);
        }
        unsafe {
            res = arr.assume_init();
        }
    }

    let ones = Array1::<f64>::ones(total_len);
    res.diag_mut().assign(&ones);
    Ok(res)
}

fn girr_bucket_kb_sb<'a>(bucket_df: &'a DataFrame, 
    girr_delta_rho_same_curve: &Array2<f64>, 
    girr_delta_rho_diff_curve: &Array2<f64>,
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,) -> Result<((&'a str, f64), f64)> {

    let bucket = unsafe{bucket_df["b"].utf8()?.get_unchecked(0).unwrap_or_else(||"Default")};
    
    // Extracting yield curves
    let yield_arr_mask = bucket_df.column("rft")?.equal("Yield")?;
    let yield_arr = bucket_df
        .filter(&yield_arr_mask)?
        .select(["y025", "y05", "y1", "y2", "y3", "y5", "y10", "y15", "y20", "y30"])?
        .to_ndarray::<Float64Type>()?;
    // Reshape in order to perform matrix multiplication
    let yield_arr_shaped = yield_arr
        .to_shape((yield_arr.len(), Order::RowMajor) )
        .map_err(|_| PolarsError::ShapeMisMatch("Could not reshape yield_arr".into()) )?;
    // Extracting Infl curves
    let infl_mask = bucket_df.column("rft")?.equal("Inflation")?;
    // No need to reshape since already in good shape
    let infl_df = bucket_df
        .filter(&infl_mask)?;
    let infl_arr = infl_df["y0"].f64()?
        .to_ndarray()?;
    // Extracting XCCY curves
    let xccy_mask = bucket_df.column("rft")?.equal("XCCY")?;
    // No need to reshape since already in good shape
    let xccy_df = bucket_df
        .filter(&xccy_mask)?;
    let xccy_arr = xccy_df["y0"].f64()?
        .to_ndarray()?;
    
    let corr_mtrx: Array2<f64> = build_girr_corr_matrix(girr_delta_rho_same_curve, girr_delta_rho_diff_curve, 
        girr_delta_rho_infl, girr_delta_rho_xccy, 
        yield_arr.len(), infl_arr.len(), xccy_arr.len())?;
        
    let yield_infl_xccy: Array1<f64> = 
        ndarray::concatenate(Axis(0), &[yield_arr_shaped.view(),
         infl_arr, xccy_arr] )
        .map_err(|_| PolarsError::ShapeMisMatch("Could not concatenate yield infl xccy".into()) )?;

    let a = yield_infl_xccy.dot(&corr_mtrx);

    //21.4.4
    let kb = a.dot(&yield_infl_xccy)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = yield_infl_xccy.sum();

    Ok(((bucket, kb), sb))
}
