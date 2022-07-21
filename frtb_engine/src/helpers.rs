use crate::prelude::*;

use ndarray::{prelude::*, Zip};
use ndarray::Order;
use polars::prelude::*;
use rayon::prelude::*;
use log::warn;

/// Shifts 2D array by {int} to the right
/// Creates a new Array2 (essentially cloning)
#[allow(dead_code)]
pub(crate)fn shift_right_by(by: usize, a: &Array2<f64>) -> Array2<f64> {
    
    // if shift_by is > than number of columns
    let x: isize = ( by % a.len_of(Axis(1)) ) as isize;

    // if shift by 0 simply return the original
    if x == 0 {
        return a.clone()
    }
    // create an uninitialized array
    let mut b = Array2::uninit(a.dim());

    // x first columns in b are two last in a
    // rest of columns in b are the initial columns in a
    a.slice(s![.., -x..]).assign_to(b.slice_mut(s![.., ..x]));
    a.slice(s![.., ..-x]).assign_to(b.slice_mut(s![.., x..]));

    // Now we can promise that `b` is safe to use with all operations
    unsafe {
        b.assume_init()
    }
}

// Identifies NON (nan or zero) indexes
pub fn non_nan_zero_idxs(arr: ArrayView1<f64>) -> Vec<usize> {
    let mut nans: Vec<usize> = vec![];
    for (i, n) in arr.indexed_iter() {
        if !(n.is_nan()|(*n==0.)) {nans.push(i)};
    };
    nans
}

#[allow(dead_code)]
#[deprecated(note="Use bucket_kb_sb_chunks instead")]
/// Builds commodity: basis(location) and commodity(riskFactor) rhos
/// And name/basis for CSR
/// And potentially others
/// Removing unwanted indexes is slowing down the code massively
pub(crate) fn build_basis_rho(n_tenors: usize, srs: &Series, rho_diff: f64) -> Result<Array2<f64>> {
    let ln = srs.len();
    let loc_chunkarray = srs.utf8()?;
    //Keep building on each request, this is to be able to easily modify parameters
    let rho_base_same = Array2::<f64>::ones((n_tenors, n_tenors));
    let rho_base_diff = Array2::<f64>::from_elem((n_tenors, n_tenors), rho_diff);
    let mut vec_rows_cols: Vec<Vec<ArrayView2<f64>>> = vec![];

    for i in 0..ln {
        let rf_i = unsafe{ loc_chunkarray.get_unchecked(i).unwrap() };
        let rf_vec: Vec<ArrayView2<f64>> = loc_chunkarray
            .par_iter() //par_iter because len of srs might be large
            .map(|x| match x {
                Some(rf2) if rf2==rf_i => rho_base_same.view() ,
                _ => rho_base_diff.view()
            })
            .collect();
        vec_rows_cols.push(rf_vec)
    }

    let vec_rows: Vec<Array2<f64>> = vec_rows_cols
        .par_iter()
        .map(|x| 
            ndarray::concatenate(Axis(1), x).unwrap()
            //.select(Axis(1), idx_select)
        )
        .collect();
    
    let rho_basis = ndarray::concatenate(Axis(0), vec_rows
            .iter()
            .map(|x| x.view())
            .collect::<Vec<ArrayView2<f64>>>()
            .as_slice())
        //.and_then(|x| 
            //Ok(x.select(Axis(0), idx_select))
        //    Ok(x)
        //)
        .map_err(|_| PolarsError::ShapeMisMatch("Could not build Commodity Basis Rho. Invalid Shape".into()));

    rho_basis
}

/// if CRR2 feature is not activated, this will return BCBS
/// if jurisdiction is not part of optional params or can't parse this will return BCBS
pub(crate) fn get_jurisdiction(op: &OCP) -> Jurisdiction {
    op.as_ref()
    .and_then(|map| map.get("jurisdiction"))
    .and_then(|x| x.parse::<Jurisdiction>().ok())
    //.unwrap()
    .unwrap_or_else(||{
        warn!("Jurisdiction defaulted to: BCBS");
        Jurisdiction::default()
    })
}

pub(crate) fn across_bucket_agg<I: IntoIterator<Item = f64>>(kbs: I, sbs: I, gamma: &Array2<f64>, res_len: usize) 
-> Result<Series>
 {
    let kbs_arr = Array1::from_iter(kbs);        
    let sbs_arr = Array1::from_iter(sbs);

    //21.4.5 sum{ sum {gamma*s_b*s_c} }
    let a = sbs_arr.dot(gamma);
    let b = a.dot(&sbs_arr);

    //21.4.5 sum{K-b^2}
    let c = kbs_arr.dot(&kbs_arr);

    let sum = c+b;

    let res = if sum < 0. {
        //21.4.5.b
        let sbs_alt = alt_sbs(sbs_arr.view(), kbs_arr.view());
        //now recalculate capital charge with alternative sb
        //21.4.5 sum{ sum {gamma*s_b*s_c} }
        let a = sbs_alt.dot(gamma);
        let b = a.dot(&sbs_alt);
        //21.4.5 sum{K-b^2}
        let c = kbs_arr.dot(&kbs_arr);
        let sum = c+b;
        sum.sqrt()
    } else {
        sum.sqrt()
    };

    // The function is supposed to return a series of same len as the input, hence we broadcast the result
    let res_arr = Array::from_elem(res_len, res);
    // if option panics on .unwrap() implement match and use .iter() and then Series from iter
    Ok( Series::new("res", res_arr.as_slice().unwrap() ) )
}

pub(crate) fn alt_sbs(sbs_arr: ArrayView1<f64>, kbs_arr: ArrayView1<f64>) -> Array1<f64>{
   //21.4.5.b
   let mut sbs_alt = Array1::<f64>::zeros(kbs_arr.raw_dim());
   Zip::from(&mut sbs_alt)
       .and(sbs_arr)
       .and(kbs_arr)
       .par_for_each(|alt, &sb, &kb|{
           let _min = sb.min(kb);
           *alt = _min.max(-kb);
   });
   sbs_alt
}

/// Common function used for CSR nonSec, Commodity, CSR CTP
/// Computes kb and sb efficiently via uninit
pub(crate) fn bucket_kb_sb_chunks<F>(df: LazyFrame, bucket_id: usize, special_bucket: Option<usize>, 
    rho_tenor: &Array2<f64>, rho_bucket: Vec<f64>, rho_basis: f64, scenario_fn: F,
    tenor_cols:Vec<&str>,bucket_col_name: &'static str, basis_col_name: &'static str) 
-> Result<(f64, f64)> 
where F: Fn(f64) -> f64 + Sync + Send + 'static,{
    let bucket_df = df//.lazy()
            .filter(col("b").eq(lit(bucket_id.to_string())))
            .collect()?;

    let n_curves = bucket_df.height();
    if bucket_df.height() == 0 { return Ok((0.,0.)) };

    let n_tenors = tenor_cols.len();
    let mut ws_arr = bucket_df
                .select(tenor_cols)?
                .to_ndarray::<Float64Type>()?;
    // 21.56 
    match special_bucket {
        Some(x) if x==bucket_id => {
            ws_arr.par_iter_mut().for_each(|x|*x=x.abs());
            return Ok((ws_arr.sum(),ws_arr.sum()))
        },
        _ => (),
    };

    let name_arr = bucket_df[bucket_col_name].utf8()?;
    let curve_type_arr = bucket_df[basis_col_name].utf8()?;

    let rho_name_bucket = rho_bucket[bucket_id-1];

    let mut arr = Array2::<f64>::uninit((n_curves*n_tenors, n_curves*n_tenors));
    arr
    .exact_chunks_mut((n_tenors, n_tenors))
    .into_iter()
    .enumerate()
    .par_bridge()
    .for_each(|(i, chunk)|{
        let row_id = i/n_curves; //eg 27usize/10usize = 2usize
        let col_id = i%n_curves; //eg 27usize % 10usize = 7usize
        
        let name_rho = if 
        unsafe{ name_arr.get_unchecked(row_id).unwrap_or_else(||"Default") } 
        == unsafe{ name_arr.get_unchecked(col_id).unwrap_or_else(||"Default") }{
            1.
        } else {
            rho_name_bucket
        };

        let basis_rho = if
         unsafe{ curve_type_arr.get_unchecked(row_id).unwrap_or_else(||"Default") } 
         == unsafe{ curve_type_arr.get_unchecked(col_id).unwrap_or_else(||"Default") } {
            1.
        } else {
            rho_basis
        };
        (rho_tenor*name_rho*basis_rho).move_into_uninit(chunk);
        //chunk.assign(&(rho_tenor*name_rho*basis_rho));
    });
    let mut rho: Array2<f64>;
    unsafe {
        rho = arr.assume_init();
    }

    rho.par_mapv_inplace(|el| {scenario_fn(el)});
    // Get rid of NaNs/Zeros before multiplying
    let csr_shaped = ws_arr
            .to_shape((ws_arr.len(), Order::RowMajor) )
            .map_err(|_| PolarsError::ShapeMisMatch("Could not reshape csr arr".into()) )?;
    //21.4.4
    let a = csr_shaped.dot(&rho);
    //21.4.4
    let kb = a.dot(&csr_shaped)
        .max(0.)
        .sqrt();

    //21.4.5.a
    let sb = csr_shaped.sum();
    
    Ok((kb,sb))
}