use base_engine::prelude::*;
use ndarray::Order;
use rayon::iter::ParallelBridge;
use crate::sbm::common::*;
use crate::helpers::*;
use crate::prelude::*;
use std::mem::MaybeUninit;

use polars::prelude::*;
use ndarray::prelude::*;
use ndarray::parallel::prelude::ParallelIterator;

pub fn total_ir_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("GIRR", "Delta")
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

///calculate GIRR Delta Low Capital charge
pub(crate) fn girr_delta_charge_low(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*LOW_CORR_SCENARIO)  
}

///calculate GIRR Delta Medium Capital charge
pub(crate) fn girr_delta_charge_medium(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*MEDIUM_CORR_SCENARIO)  
}

///calculate GIRR Delta High Capital charge
pub(crate) fn girr_delta_charge_high(op: &OCP) -> Expr {
    girr_delta_charge_distributor(op, &*HIGH_CORR_SCENARIO)
}

/// Helper funciton
/// Extracts relevant fields from OptionalParams
fn girr_delta_charge_distributor(op: &OCP, scenario: &'static ScenarioConfig) -> Expr {
    let _suffix = scenario.as_str();

    let girr_delta_gamma = get_optional_parameter(op, format!("girr_delta_gamma{_suffix}").as_ref() as &str, &scenario.girr_delta_gamma);

    let girr_delta_rho_same_curve = &scenario.girr_delta_rho_same_curve;
    let girr_delta_rho_diff_curve = &scenario.girr_delta_rho_diff_curve;
    let girr_delta_rho_infl = scenario.girr_delta_rho_infl;
    let girr_delta_rho_xccy = scenario.girr_delta_rho_xccy;

    girr_delta_charge(girr_delta_gamma, girr_delta_rho_same_curve, girr_delta_rho_diff_curve, girr_delta_rho_infl, girr_delta_rho_xccy)
}

/// References moved into apply_multiple have to be 'static. Hence we can't pass a ref to a newly created Array2
/// The only solution is to clone Array2 and make this function take ownership of it
/// This allows for flexibility with overriding ScenarioConfig, but the drawback is cloning
/// girr_delta_charge is called up to 3 times(1 for each scenario) FOR EACH group in outer groupby(say N) per request
/// in total 3xN clones. Performance of such many clones has to be carefully accessed
fn girr_delta_charge(girr_delta_gamma: f64, girr_delta_rho_same_curve: &'static Array2<f64>, 
    girr_delta_rho_diff_curve: &'static Array2<f64>, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64) -> Expr {

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
        
        let res_kbs_sbs: Result<Vec<(f64,f64)>> = df["b"].unique()?
            .utf8()?
            .par_iter()
            .map(|b|{
                girr_bucket_kb_sb(df.clone().lazy(), b.unwrap_or_else(||"Default"), 
                girr_delta_rho_same_curve, girr_delta_rho_diff_curve, 
                girr_delta_rho_infl, girr_delta_rho_xccy)
            })
            .collect();
        let kbs_sbs = res_kbs_sbs?;
        let (kbs, sbs): (Vec<f64>, Vec<f64>) = kbs_sbs.into_iter().unzip();
        let mut gamma = Array2::from_elem((kbs.len(), kbs.len()), girr_delta_gamma );
        let zeros = Array1::zeros(kbs.len() );
        gamma.diag_mut().assign(&zeros);

        across_bucket_agg(kbs, sbs, &gamma, columns[0].len())
    }, 
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), col("BucketBCBS"), 
    girr_delta_sens_weighted_spot(), girr_delta_sens_weighted_025y(), girr_delta_sens_weighted_05y(),
    girr_delta_sens_weighted_1y(), girr_delta_sens_weighted_2y(), girr_delta_sens_weighted_3y(),
    girr_delta_sens_weighted_5y(), girr_delta_sens_weighted_10y(), girr_delta_sens_weighted_15y(),
    girr_delta_sens_weighted_20y(), girr_delta_sens_weighted_30y(), col("RiskCategory")], 
        GetOutput::from_type(DataType::Float64))
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

fn girr_bucket_kb_sb(df: LazyFrame, bucket: &str, 
    girr_delta_rho_same_curve: &Array2<f64>, 
    girr_delta_rho_diff_curve: &Array2<f64>,
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,) -> Result<(f64, f64)> {
    let bucket_df = df
            .filter(col("b").eq(lit(bucket)))
            .collect()?;
    
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
    // No need to reshape since aLready in a good shape
    let infl_df = bucket_df
        .filter(&infl_mask)?;
    let infl_arr = infl_df["y0"].f64()?
        .to_ndarray()?;
    // Extracting XCCY curves
    let xccy_mask = bucket_df.column("rft")?.equal("XCCY")?;
    // No need to reshape since aLready in a good shape
    let xccy_df = bucket_df
        .filter(&xccy_mask)?;
    let xccy_arr = xccy_df["y0"].f64()?
        .to_ndarray()?;
    
    let corr_mtrx: Array2<f64> = build_girr_corr_matrix(girr_delta_rho_same_curve, girr_delta_rho_diff_curve, 
        girr_delta_rho_infl, girr_delta_rho_xccy, 
        yield_arr.len(), infl_arr.len(), xccy_arr.len())?;
        
    let yield_infl_xccy: Array1<f64> = 
        ndarray::concatenate(Axis(0), &[yield_arr_shaped.view(),
         infl_arr.view(), xccy_arr.view()] )
        .map_err(|_| PolarsError::ShapeMisMatch("Could not concatenate yield infl xccy".into()) )?;

    let a = yield_infl_xccy.dot(&corr_mtrx);

    //21.4.4
    let kb = a.dot(&yield_infl_xccy)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = yield_infl_xccy.sum();
    //kbs.push(kb)

    Ok((kb, sb))
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
