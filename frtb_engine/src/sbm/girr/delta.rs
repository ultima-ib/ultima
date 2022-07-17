use base_engine::prelude::*;
use ndarray::Order;
use ndarray::parallel::prelude::IntoParallelRefIterator;
use crate::sbm::common::*;
use crate::helpers::*;
use crate::prelude::*;

use polars::prelude::*;
use ndarray::{prelude::*, Zip};
use ndarray::parallel::prelude::ParallelIterator;

pub fn total_ir_delta_sens (_: &OCP) -> Expr {
    rc_delta_sens("GIRR")
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

/// Total GIRR Delta
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

    let girr_delta_gamma = op.as_ref()
        .and_then(|map| map.get(format!("girr_delta_gamma{_suffix}").as_ref() as &str))
        .and_then(|s| s.parse::<f64>().ok() )
        .unwrap_or(scenario.girr_delta_gamma);

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
fn girr_delta_charge(girr_delta_gamma: f64, girr_delta_rho_same_curve: &'static Array2<f64>, girr_delta_rho_diff_curve: &'static Array2<f64>, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64) -> Expr {

    apply_multiple( move |columns| {

        let df = df![
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
            .filter(col("rc").eq(lit("GIRR")))
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
            .collect()?;
        
        let res_kbs_sbs: Result<Vec<(f64,f64)>> = df["b"].unique()?
            .utf8()?
            .par_iter()
            .map(|b|{
                girr_bucket_kb_sb(df.clone(), b.unwrap(), 
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
    girr_delta_sens_weighted_20y(), girr_delta_sens_weighted_30y()], 
        GetOutput::from_type(DataType::Float64))
}


/// Dirty, builds correlation matrix using all 10 tenors
/// However, we select non zero idxs
/// Ideally we should build corr mtrx from views to non zero indxs (see GIRR Delta Simple tab in the xlsx)
/// However, this is not yet possible: https://github.com/rust-ndarray/ndarray/discussions/1183
fn build_girr_corr_matrix( girr_delta_rho_same_curve: &Array2<f64>, 
    girr_delta_rho_diff_curve: &Array2<f64>, 
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,
    y: usize, i: usize, x: usize,
    yield_idx_select: &[usize]) -> Result<Array2<f64>> {

    let n = 10; //represents number of tenors (aka columns) of yield curve
    let y_curves = y / n ; //represents number of yield curves
    let i_curves = i;
    let x_curves = x;

    let mut res = Array2::ones((0, 0));

    if y_curves > 0 {
        //21.46 GIRR delta risk correlation within same bucket, same curve
        let corr_with_self_yield = girr_delta_rho_same_curve;
        //21.47 GIRR delta risk correlation within same bucket, different curves
        let corr_with_other_yields = girr_delta_rho_diff_curve;//corr_with_self_yield.clone() * 0.999;
        // Vec to store a set of vecs ("rows")
        let mut vec_rows_cols: Vec<Vec<ArrayView2<f64>>> = Vec::with_capacity(y_curves);

        for i in 0..y_curves {
            let mut tmp: Vec<ArrayView2<f64>> = Vec::with_capacity(y_curves);
            tmp.extend(vec![corr_with_other_yields.view(); i]);
            tmp.extend([corr_with_self_yield.view()]);
            tmp.extend(vec![corr_with_other_yields.view(); y-i-1]);
            vec_rows_cols.push(tmp);
        };

        let vec_rows: Vec<Array2<f64>> = vec_rows_cols
        .par_iter()
        .map(|x| ndarray::concatenate(Axis(1), x)
            .unwrap() // have to unwrap here since inside closure
            .select(Axis(1), yield_idx_select))
        .collect();

        res = ndarray::concatenate(Axis(0), vec_rows
            .iter()
            .map(|x| x.view())
            .collect::<Vec<ArrayView2<f64>>>()
            .as_slice())
        .and_then(|x| Ok(x.select(Axis(0), yield_idx_select)))
        .map_err(|_| PolarsError::ShapeMisMatch("Could not build Yield Rho. Invalid Shape".into()))?;

    }

    for _ in 0..i_curves {
        let res_nrows = res.nrows();
        // 21.48
        let v_col = vec![girr_delta_rho_infl; res_nrows];
        let mut v_row = v_col.clone();
        v_row.push(1.);
        res.push_column(Array1::from_vec(v_col).view())
            .map_err(|_| PolarsError::ShapeMisMatch("GIRR Inflation couldn't push column".into()) )?;
        res.push_row(Array1::from_vec(v_row).view())
            .map_err(|_| PolarsError::ShapeMisMatch("GIRR Inflation couldn't push row".into()) )?;
    }

    for _ in 0..x_curves {
        let res_nrows = res.nrows();
        //21.49
        let v_col = vec![girr_delta_rho_xccy; res_nrows];
        let mut v_row = v_col.clone();
        v_row.push(1.);
        res.push_column(Array1::from_vec(v_col).view())
            .map_err(|_| PolarsError::ShapeMisMatch("GIRR Xccy couldn't push column".into()) )?;
        res.push_row(Array1::from_vec(v_row).view())
            .map_err(|_| PolarsError::ShapeMisMatch("GIRR Xccy couldn't push row".into()) )?;
    }
    // can't select using all indexes anymore, since yield nan/zero indexes have already been dropped
    // however, that's ok since generally if XCCY or Inflation is provided, the Spot risk shouldn't be 0.
    Ok(res)
}


///21.46 GIRR delta risk correlation within same bucket, same curve
/// results in 10x10 matrix
/// Used at the initiation of the program using OnceCell
pub(crate) fn girr_corr_matrix() -> Array2<f64> {
    let mut base_weights = Array2::<f64>::zeros((10, 10));
    let tenors = [ 0.25, 0.5, 1., 2., 3., 5., 10., 15., 20., 30. ];

    for ((row, col), val) in base_weights.indexed_iter_mut() {
        let tr = tenors[row];
        let tc = tenors[col];
        *val = f64::max( f64::exp( -0.03 * f64::abs(tr-tc) / tr.min(tc) ) , 0.4 );
    }
    base_weights
}

fn girr_bucket_kb_sb(df: DataFrame, bucket: &str, 
    girr_delta_rho_same_curve: &Array2<f64>, 
    girr_delta_rho_diff_curve: &Array2<f64>,
    girr_delta_rho_infl: f64, girr_delta_rho_xccy: f64,) -> Result<(f64, f64)> {
    let bucket_df = df.lazy()
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
    
    let non_nan_zero_idxs_yield_vec =  non_nan_zero_idxs(yield_arr_shaped.view());

    let corr_mtrx: Array2<f64> = build_girr_corr_matrix(girr_delta_rho_same_curve, girr_delta_rho_diff_curve, 
        girr_delta_rho_infl, girr_delta_rho_xccy, 
        yield_arr.len(), infl_arr.len(), xccy_arr.len(),
        &non_nan_zero_idxs_yield_vec)?;
        
    // Concat into one vector, note:
    // Rhos has been reduced by non nan/zero yields already. 
    // Now, reduce weighted deltas by throwing away zeros and nans of YIELD ONLY
    let yield_infl_xccy: Array1<f64> = 
        ndarray::concatenate(Axis(0), &[yield_arr_shaped.select(Axis(0), &non_nan_zero_idxs_yield_vec).view(),
         infl_arr.view(), xccy_arr.view()] )
        .map_err(|_| PolarsError::ShapeMisMatch("Could not concatenate yield infl xccy".into()) )?;
    //Rhos has been reduced already. 
    // Note above we selected non nan/zero YIELD indexes only
    //Now, reduce weighted deltas by throwing away zeros and nans of YIELD ONLY
   // yield_infl_xccy = yield_infl_xccy.select(Axis(0), &non_nan_zero_idxs_yield_vec);

    let a = yield_infl_xccy.t().dot(&corr_mtrx);

    //21.4.4
    let kb = a.dot(&yield_infl_xccy)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = yield_infl_xccy.sum();
    //kbs.push(kb)

    Ok((kb, sb))
}

