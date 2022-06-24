//! FRTB job entry point
pub mod prelude;
mod sbm;

use prelude::sbm::buckets;
use sbm::delta_weights::*;
use base_engine::prelude::*;

use lazy_static::lazy_static;
use std::collections::HashMap;
use polars::prelude::*;
use serde::{Serialize, Deserialize};
use ndarray::{prelude::*, Zip};
use ndarray::{Array, Array2, concatenate};
use std::cell::RefCell;


pub trait FRTBDataSetT {
    fn prepare(self) -> Self;
}

impl FRTBDataSetT for DataSet {
    fn prepare(mut self) -> Self {

        if self.f1.height() != 0 {
            let lf1 = self.f1.lazy()
            .with_column(
                buckets::sbm_buckets()
            )
            .with_columns(&[
                delta_weights_assign().alias("DeltaWeights")
            ]);
            self.f1 = lf1.collect().unwrap();
        }
        self
    }
    // @TODO Validate:
    // all columns are present
    // columns are of expected format 
    // if risk class == GIRR => Risk Factor Type is not empty and one of:
    // Yield, XCCY, Inflation
    //fn validate(self) -> Self {}
}

/// Sum of all delta sensis, from spot to 30Y tenor
/// In practice should be used only with filter on RiskClass
/// FX and IR sensis is meaningless
pub fn total_delta_sens() -> Expr {
    col("DeltaSpot") + col("Delta_025Y") +
    col("Delta_05Y") + col("Delta_1Y")+ col("Delta_2Y") +
    col("Delta_3Y") + col("Delta_5Y") + col("Delta_10Y") + 
    col("Delta_15Y") + col("Delta_20Y") + col("Delta_30Y")
}

/// Returns a Series equal to DeltaSpot with RiskClass == FX and RiskFactor == ...CCY where CCY is Reporting CCY
/// @TODO: function should take Some(FRTBParams as an argument)
/// as we want to support different reporting currencies
pub fn fx_delta_sens () -> Expr {
    // Add reporting_ccy check here
    apply_multiple(|columns| {
        let mask1 = columns[0]
            .utf8()?
            .contains("FX")?;
        
        // temp filtering on just USD
        // function to take rep_ccy as an argument
        let mask2 = columns[1]
            .utf8()?
            .contains("^...USD$")?;
        
        let delta = columns[2]
            .f64()?
            .set(&!(mask1&mask2), None)?;

        Ok(delta.into_series())
    }, 
        &[col("RiskClass"), col("RiskFactor"), col("DeltaSpot")], 
        GetOutput::from_type(DataType::Float64))
}

pub fn fx_delta_sens_weighted () -> Expr {
    fx_delta_sens() * col("DeltaWeights").arr().get(0)
}

pub fn ir_delta_sens () -> Expr {
    rc_delta_sens("GIRR")
}

/// WhenThen shouldn't be used inside groupby
/// but this works so far
pub fn rc_delta_sens(rc: &str) -> Expr {
    when(col("RiskClass").eq(lit(rc)))
    .then(total_delta_sens()) 
    .otherwise(lit::<f64>(0.0))
}

/// Helper function to derive weighted delta,
/// per tenor, per risk class.
pub fn rc_delta_sens_weighted(rc: &'static str, delta_tenor: &str, idx: i64) -> Expr {

    apply_multiple(|columns| {

        let mask = columns[0]
            .utf8()?
            .contains(rc)?;
        
        let delta = columns[1]
            .f64()?
            .set(&!mask.clone(), None)?;

        let weight = columns[2]
            .f64()?
            .set(&!mask, None)?;

        let x = delta * weight;
        Ok(x.into_series())
    }, 
        &[col("RiskClass"), col(delta_tenor), col("DeltaWeights").arr().get(idx)], 
        GetOutput::from_type(DataType::Float64))
}

/// Helper functions
pub fn girr_delta_sens_weighted_spot() -> Expr {
    rc_delta_sens_weighted( "GIRR", "DeltaSpot", 0)
}
pub fn girr_delta_sens_weighted_025y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_025Y", 1)
}
pub fn girr_delta_sens_weighted_05y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_05Y", 2)
}
pub fn girr_delta_sens_weighted_1y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_1Y", 3)
}
pub fn girr_delta_sens_weighted_2y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_2Y", 4)
}
pub fn girr_delta_sens_weighted_3y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_3Y", 5)
}
pub fn girr_delta_sens_weighted_5y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_5Y", 6)
}
pub fn girr_delta_sens_weighted_10y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_10Y", 7)
}
pub fn girr_delta_sens_weighted_15y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_15Y", 8)
}
pub fn girr_delta_sens_weighted_20y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_20Y", 9)
}
pub fn girr_delta_sens_weighted_30y() -> Expr {
    rc_delta_sens_weighted("GIRR", "Delta_30Y", 10)
}


///makes sence at RiskClass-Bucket view
pub fn delta_weights() -> Expr {
    col("DeltaWeights")
}

///calculate FX Delta High Capital charge
pub fn fx_delta_charge_high() -> Expr {
    fx_delta_charge(0.75)
}

///calculate FX Delta Medium Capital charge
pub fn fx_delta_charge_medium() -> Expr {
    fx_delta_charge(0.6)
}


///calculate FX Delta Medium Capital charge
pub fn fx_delta_charge_low() -> Expr {
    fx_delta_charge(0.45)
}

///calculate FX Delta Capital charge
pub fn fx_delta_charge(gamma: f64) -> Expr {
    // inner function
    apply_multiple( move |columns| {

        let df = df![
            "rc" => columns[0].clone(), 
            "rf" => columns[1].clone(),
            "dw" => columns[2].clone(),
        ]?;
        

        let df = df.lazy()
            .filter(col("rc").eq(lit("FX")).and(
                col("dw").is_not_null()
            ))
            .groupby([col("rf")])
            .agg([col("dw").sum().alias("dw_sum")])
            .collect()?;
        
        //21.4.4 |dw_sum| == kb for FX
        //21.4.5.a sb == dw_sum
        let dw_sum = df["dw_sum"].f64()?
            .to_ndarray()?;

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
    &[ col("RiskClass"), col("RiskFactor"), fx_delta_sens_weighted() ], 
        GetOutput::from_type(DataType::Float64))

}

///calculate GIRR Delta Low Capital charge
pub fn girr_delta_charge_low() -> Expr {
    girr_delta_charge(&*LOW_CORR_SCENARIO)
}

///calculate GIRR Delta Medium Capital charge
pub fn girr_delta_charge_medium() -> Expr {
    girr_delta_charge(&*MEDIUM_CORR_SCENARIO)
}

///calculate GIRR Delta High Capital charge
pub fn girr_delta_charge_high() -> Expr {
    girr_delta_charge(&*HIGH_CORR_SCENARIO)
}

fn girr_delta_charge(scenario: &'static ScenarioConfig) -> Expr {
    // inner function
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
        println!("Inner DF agged: {:?}", df);

        let mut kbs: Vec<f64> = vec![];
        // sb = sum {ws_b}
        let mut sbs: Vec<f64> = vec![];

        for bucket_df in df.partition_by(["b"])? {

            dbg!(bucket_df.clone());

            let ultra_yield = RefCell::new(Array1::<f64>::zeros(0));
            let ultra_infl = RefCell::new(Array1::<f64>::zeros(0));
            let ultra_xccy = RefCell::new(Array1::<f64>::zeros(0));
            
            for rtf_bucket_df in bucket_df.partition_by(["rft"])? {
                if let AnyValue::Utf8( rftype_str) =  rtf_bucket_df["rft"].get(0) {
                    match rftype_str {
                        "Yield" => {
                            let yield_arr = rtf_bucket_df
                                .select(["y025", "y05", "y1", "y2", "y3", "y5", "y10", "y15", "y20", "y30"])?
                                .to_ndarray::<Float64Type>()?;
                            let yield_reshaped = yield_arr.to_shape((1, yield_arr.len()) )
                                .unwrap()
                                .row(0)
                                .to_owned();
                            
                            *ultra_yield.borrow_mut() = yield_reshaped;
                        },
                        "Inflation" => {
                            let infl_arr = rtf_bucket_df["y0"]
                                .f64()?
                                .to_ndarray()?
                                .to_owned();

                            *ultra_infl.borrow_mut() = infl_arr;
                        },
                        "XCCY" => {
                            let xccy_arr = rtf_bucket_df["y0"]
                                .f64()?
                                .to_ndarray()?
                                .to_owned();

                            *ultra_xccy.borrow_mut() = xccy_arr;
                        },
                        _ => continue//return PolarsError::SchemaMisMatch("Risk Factor Type of GIRR Risk Class can be one of: Yield, Inflation, XCCY").into(),
                    }
                } else {
                    continue
                }
            }
            let y = ultra_yield.take();
            let i = ultra_infl.take();
            let x = ultra_xccy.take();

            let corr_mtrx = build_girr_corr_matrix(scenario, &y, &i, &x);
            let mut yield_infl_xccy = ndarray::concatenate(Axis(0), &[y.view(), i.view(), x.view()] )
                .unwrap();
            yield_infl_xccy.par_mapv_inplace(|x| if x.is_nan() { 0. } else { x } );

            let a = yield_infl_xccy.t().dot(&corr_mtrx);

            //21.4.4
            let kb = a.dot(&yield_infl_xccy)
                .max(0.)
                .sqrt();

            println!("##########################GIRR Kb: {}", kb);
            //21.4.5.a
            sbs.push(yield_infl_xccy.sum());
            kbs.push(kb)
        }
        let sbs_arr = Array1::from_vec(sbs);
        let kbs_arr = Array1::from_vec(kbs);

        let mut gamma_m = Array2::from_elem((kbs_arr.len(), kbs_arr.len()), scenario.girr_delta_gamma );
        let zeros = Array1::zeros(kbs_arr.len() );
        gamma_m.diag_mut().assign(&zeros);

        //21.4.5 sum{ sum {gamma*s_b*s_c} }
        let a = sbs_arr.t().dot(&gamma_m);
        let b = a.dot(&sbs_arr);

        //21.4.5 sum{K-b^2}
        let c = kbs_arr.dot(&kbs_arr);

        let sum = c+b;

        let res = if sum < 0. {
            //21.4.5.b
            let mut sbs_alt = Array1::<f64>::zeros(kbs_arr.raw_dim());
            Zip::from(&mut sbs_alt)
                .and(&sbs_arr)
                .and(&kbs_arr)
                .par_for_each(|alt, &sb, &kb|{
                    let _min = sb.min(kb);
                    *alt = _min.max(-kb);
            });
            //now recalculate capital charge with alternative sb
            //21.4.5 sum{ sum {gamma*s_b*s_c} }
            let a = sbs_alt.t().dot(&gamma_m);
            let b = a.dot(&sbs_alt);
            //21.4.5 sum{K-b^2}
            let c = kbs_arr.dot(&kbs_arr);
            let sum = c+b;
            sum.sqrt()
        } else {
            sum.sqrt()
        };

        // The function is supposed to return a series of same len as the input, hence we broadcast the result
        let res_arr = Array::from_elem(columns[0].len(), res);
        // if option panics on .unwrap() implement match and use .iter() and then Series from iter
        Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
    }, 
    &[ col("RiskClass"), col("RiskFactor"), col("RiskFactorType"), col("Bucket"), 
    girr_delta_sens_weighted_spot(), girr_delta_sens_weighted_025y(), girr_delta_sens_weighted_05y(),
    girr_delta_sens_weighted_1y(), girr_delta_sens_weighted_2y(), girr_delta_sens_weighted_3y(),
    girr_delta_sens_weighted_5y(), girr_delta_sens_weighted_10y(), girr_delta_sens_weighted_15y(),
    girr_delta_sens_weighted_20y(), girr_delta_sens_weighted_30y()], 
        GetOutput::from_type(DataType::Float64))
}

/// Dirty, builds correlation matrix using all 10 tenors
/// returns nx10 by nx10 correlation matrix
/// Potential optimization keep only tenors != NULL
/// and build correlation matrix accordingly (see GIRR Delta Simple)
fn build_girr_corr_matrix(scenario: &ScenarioConfig, y: &Array1<f64>, i: &Array1<f64>, x: &Array1<f64>) -> Array2<f64> {
    let n = 10; //represents number of tenors (columns per curve)
    let y_len = y.len() / n ;
    let i_len = i.len();
    let x_len = x.len();

    let mut res = Array2::ones((0, 0));

    if y_len > 0 {
        //21.46 GIRR delta risk correlation within same bucket, same curve
        let corr_with_self_yield = &scenario.girr_delta_rho_same_curve;
        //21.47 GIRR delta risk correlation within same bucket, different curves
        let corr_with_other_yields = &scenario.girr_delta_rho_diff_curve;//corr_with_self_yield.clone() * 0.999;
        //dbg!(&corr_with_other_yields);

        let vec_corr_with_other_curves = vec![corr_with_other_yields.view(); y_len -1 ];
        let mut vec_yield_corr_row1 = vec![corr_with_self_yield.view()];
        //This gives first horizontal block of correlation matrix
        //This is a block of 10x10 matrixes, where first one is the "base" (corr with self) and the rest are "base"*0.999
        vec_yield_corr_row1.extend(vec_corr_with_other_curves);
        let arr_yield_corr_row1 = ndarray::concatenate(Axis(1), vec_yield_corr_row1.as_slice()).unwrap();

        res = arr_yield_corr_row1.clone();
        for j in 1..y_len {
        //for each curve shift the block by 10 and then add to res as rows
        res = concatenate![Axis(0), res, shift_columns_by(j*n, &arr_yield_corr_row1)]
        };
    }

    for _ in 0..i_len {
        let res_nrows = res.nrows();
        // 21.48
        let v_col = vec![scenario.girr_delta_rho_infl; res_nrows];
        let mut v_row = v_col.clone();
        v_row.push(1.);
        res.push_column(Array1::from_vec(v_col).view());
        res.push_row(Array1::from_vec(v_row).view());
    }

    for _ in 0..x_len {
        let res_nrows = res.nrows();
        //21.49
        let v_col = vec![scenario.girr_delta_rho_xccy; res_nrows];
        let mut v_row = v_col.clone();
        v_row.push(1.);
        res.push_column(Array1::from_vec(v_col).view());
        res.push_row(Array1::from_vec(v_row).view());
    }
    res
    //dbg!(res)
}


///21.46 GIRR delta risk correlation within same bucket, same curve
/// results in 10x10 matrix
fn girr_corr_matrix() -> Array2<f64> {
    let mut base_weights = Array2::<f64>::zeros((10, 10));
    let tenors = [ 0.25, 0.5, 1., 2., 3., 5., 10., 15., 20., 30. ];

    for ((row, col), val) in base_weights.indexed_iter_mut() {
        let tr = tenors[row];
        let tc = tenors[col];
        *val = f64::max( f64::exp( -0.03 * f64::abs(tr-tc) / tr.min(tc) ) , 0.4 );
    }
    base_weights
}

/// Helper
/// Shifts 2D array by {int} to the right
fn shift_columns_by(by: usize, a: &Array2<f64>) -> Array2<f64> {
    
    // if shift_by is > than number of columns
    let x: isize = ( by % a.len_of(Axis(1)) ) as isize;

    // if shift by 0 simply return the original
    if x == 0 {
        return a.clone()
    }
    // create an uninitialized array
    let mut b = Array2::uninit(a.dim());

    // two first columns in b are two last in a
    // rest of columns in b are the initial columns in a
    a.slice(s![.., -x..]).assign_to(b.slice_mut(s![.., ..x]));
    a.slice(s![.., ..-x]).assign_to(b.slice_mut(s![.., x..]));

    // Now we can promise that `b` is safe to use with all operations
    unsafe {
        b.assume_init()
    }
}

use once_cell::sync::Lazy;

static LOW_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
    let mut girr_corr_matrix_low_self = MEDIUM_CORR_SCENARIO.girr_delta_rho_same_curve.clone();
    girr_corr_matrix_low_self.par_mapv_inplace(|x|
        (2.*x - 1.).max(0.75*x)
    );
    let mut girr_corr_matrix_low_other = MEDIUM_CORR_SCENARIO.girr_delta_rho_diff_curve.clone();
    girr_corr_matrix_low_other.par_mapv_inplace(|x|
        (2.*x - 1.).max(0.75*x)
    );
    let mut fx_delta_gamma_low = MEDIUM_CORR_SCENARIO.fx_delta_gamma;
    fx_delta_gamma_low = (2.*fx_delta_gamma_low - 1.).max(0.75*fx_delta_gamma_low);
    let mut girr_delta_gamma_low = MEDIUM_CORR_SCENARIO.girr_delta_gamma;
    girr_delta_gamma_low = (2.*girr_delta_gamma_low - 1.).max(0.75*girr_delta_gamma_low);
    //dbg!(girr_delta_gamma_low);
    ScenarioConfig {
        scenario: "Low".to_string(),
        girr_delta_rho_same_curve: girr_corr_matrix_low_self,
        girr_delta_rho_diff_curve: girr_corr_matrix_low_other,
        girr_delta_rho_infl: (2.*MEDIUM_CORR_SCENARIO.girr_delta_rho_infl - 1.).max(0.75*MEDIUM_CORR_SCENARIO.girr_delta_rho_infl),
        girr_delta_rho_xccy: (2.*MEDIUM_CORR_SCENARIO.girr_delta_rho_xccy - 1.).max(0.75*MEDIUM_CORR_SCENARIO.girr_delta_rho_xccy),
        girr_delta_gamma: girr_delta_gamma_low,

        fx_delta_rho: 1.,
        fx_delta_gamma: fx_delta_gamma_low,
    }
});

static MEDIUM_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(||
        ScenarioConfig {
            scenario: "Medium".to_string(),

            girr_delta_rho_same_curve: girr_corr_matrix(),
            girr_delta_rho_diff_curve: girr_corr_matrix()*0.999,
            girr_delta_rho_infl: 0.4,
            girr_delta_rho_xccy: 0.,
            girr_delta_gamma: 0.5,

            fx_delta_rho: 1.,
            fx_delta_gamma: 0.6,
        }
);

static HIGH_CORR_SCENARIO: Lazy<ScenarioConfig>  = Lazy::new(|| {
    let mut girr_corr_matrix_high_self = MEDIUM_CORR_SCENARIO.girr_delta_rho_same_curve.clone();
    girr_corr_matrix_high_self.par_mapv_inplace(|x|
        (1.25*x).min(1.)
    );
    let mut girr_corr_matrix_high_other = MEDIUM_CORR_SCENARIO.girr_delta_rho_diff_curve.clone();
    girr_corr_matrix_high_other.par_mapv_inplace(|x|
        (1.25*x).min(1.)
    );
    let mut fx_delta_gamma_high = MEDIUM_CORR_SCENARIO.fx_delta_gamma;
    fx_delta_gamma_high = (1.25*fx_delta_gamma_high).min(1.);
    let mut girr_delta_gamma_high = MEDIUM_CORR_SCENARIO.girr_delta_gamma;
    girr_delta_gamma_high = (1.25*girr_delta_gamma_high ).min(1.);
    dbg!(girr_delta_gamma_high);
    ScenarioConfig {
        scenario: "Low".to_string(),

        girr_delta_rho_same_curve: girr_corr_matrix_high_self,
        girr_delta_rho_diff_curve: girr_corr_matrix_high_other,
        girr_delta_rho_infl: (1.25*MEDIUM_CORR_SCENARIO.girr_delta_rho_infl).min(1.),
        girr_delta_rho_xccy: (1.25*MEDIUM_CORR_SCENARIO.girr_delta_rho_xccy).min(1.),

        girr_delta_gamma: girr_delta_gamma_high,
        fx_delta_rho: 1.,
        fx_delta_gamma: fx_delta_gamma_high,
    }
});

// Corresponds to three correlation scenarios
// 21.6
enum CorrScenario {
    High(ScenarioConfig),
    Medium(ScenarioConfig),
    Low(ScenarioConfig),
}

// This struct to keep all parameters, per Corr Scenario
// Weights, Gammas, Rhos etc
struct ScenarioConfig {
    pub scenario: String,

    pub girr_delta_rho_same_curve: Array2<f64>,
    pub girr_delta_rho_diff_curve: Array2<f64>,
    pub girr_delta_rho_infl: f64,
    pub girr_delta_rho_xccy: f64,
    pub girr_delta_gamma: f64,
    pub fx_delta_rho: f64,
    pub fx_delta_gamma: f64,
}
// Export measures
static FX_DELTA_SENS: Lazy<Measure>  = Lazy::new(|| {
    Measure{
        calculator: Box::new(total_delta_sens),
        name: "FXDeltaSens",
        req_columns: vec!["RiskClass", "DeltaSpot"],
        aggregation: Some("sum"),
    }
});

lazy_static!(
    pub static ref MEASURE_COL_MAP: HashMap<&'static str, (Vec<&'static str>, fn() -> Expr )> 
    = HashMap::from(
        [

            ("TotalDeltaSens", 
            (vec!["DeltaSpot", "Delta_025Y", "Delta_05Y", "Delta_1Y",
             "Delta_2Y", "Delta_3Y", "Delta_5Y", "Delta_10Y", "Delta_15Y", "Delta_20Y", "Delta_30Y"],
             total_delta_sens as fn() -> Expr)),

            ("FXDeltaSens", (vec!["RiskClass", "DeltaSpot"], fx_delta_sens)),

            ("DeltaWeights", (vec!["DeltaWeights"], delta_weights)),

            //("DeltaSensWeightedSpot", (vec!["RiskClass", "RiskFactor", "DeltaSpot", "DeltaWeights"], delta_sens_weighted_spot)),

            ("FxDeltaSensWeighted", (vec!["RiskClass", "RiskFactor", "DeltaSpot", "DeltaWeights"], fx_delta_sens_weighted)),

            ("FxDeltaChargeMedium", (vec!["RiskClass", "RiskFactor", "DeltaSpot", "DeltaWeights"], fx_delta_charge_medium)),

            ("FxDeltaChargeHigh", (vec!["RiskClass", "RiskFactor", "DeltaSpot", "DeltaWeights"], fx_delta_charge_high)),

            ("FxDeltaChargeLow", (vec!["RiskClass", "RiskFactor", "DeltaSpot", "DeltaWeights"], fx_delta_charge_low)),

            ("GIRRDeltaChargeMedium", (vec!["RiskClass", "RiskFactor", "RiskFactorType","Bucket", "DeltaSpot", "DeltaWeights", "DeltaSpot", "Delta_025Y", "Delta_05Y", "Delta_1Y",
            "Delta_2Y", "Delta_3Y", "Delta_5Y", "Delta_10Y", "Delta_15Y", "Delta_20Y", "Delta_30Y"], girr_delta_charge_medium)),

            ("GIRRDeltaChargeHigh", (vec!["RiskClass", "RiskFactor", "RiskFactorType","Bucket", "DeltaSpot", "DeltaWeights", "DeltaSpot", "Delta_025Y", "Delta_05Y", "Delta_1Y",
            "Delta_2Y", "Delta_3Y", "Delta_5Y", "Delta_10Y", "Delta_15Y", "Delta_20Y", "Delta_30Y"], girr_delta_charge_high)),

            ("GIRRDeltaChargeLow", (vec!["RiskClass", "RiskFactor", "RiskFactorType","Bucket", "DeltaSpot", "DeltaWeights", "DeltaSpot", "Delta_025Y", "Delta_05Y", "Delta_1Y",
            "Delta_2Y", "Delta_3Y", "Delta_5Y", "Delta_10Y", "Delta_15Y", "Delta_20Y", "Delta_30Y"], girr_delta_charge_low)),

        ]
    );
);

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

/// To be moved to Request?
#[derive(Serialize, Deserialize, Debug)]
pub struct FRTBRegParams {
    #[serde(default="true_func")]
    girr_rw_div: bool, // 21.44
    #[serde(default="true_func")]
    fx_rw_div: bool, // 21.88
    #[serde(default="usd_func")]
    reporting_ccy: String
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ReportingCCY{
    USD,
    EUR
}

fn true_func() -> bool {
    true
}

fn usd_func() -> String {
    "USD".to_string()
}