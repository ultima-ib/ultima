use ndarray::prelude::*;
use polars::prelude::*;
use rayon::prelude::*;

/// Shifts 2D array by {int} to the right
/// Creates a new Array2 (essentially cloning)
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

pub(crate)fn shift_down_by(by: usize, a: &Array2<f64>) -> Array2<f64> {
    
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
    a.slice(s![-x.., ..]).assign_to(b.slice_mut(s![..x, ..]));
    a.slice(s![..-x, ..]).assign_to(b.slice_mut(s![x.., ..]));

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

#[deprecated(note="Better to reduce nans as building corr matrix")]
//// Helper function to remove indexes
/// Not used at as .select(non_nan_zero_idx) is preffered
fn reduce_nans(mut a: Array1<f64>, mut m: Array2<f64>) -> (Array1<f64>, Array2<f64>) {
    let mut nans: Vec<usize> = vec![];
    for (i, n) in a.indexed_iter() {
        if n.is_nan()|(*n==0.) {nans.push(i)};
    };

    for i in nans.iter().rev() {
        a.remove_index(Axis(0), *i);
        m.remove_index(Axis(0), *i);
        m.remove_index(Axis(1), *i);
    };
    (a, m)
}

/// Used to build CSR and Commodity (and potentially others) tenor rhos
///expands (n_tenorsXn_tenors) matrix into (n_curves*n_tenors X n_curves*n_tenors)
/// removing unwanted indexes
pub(crate)fn build_tenor_rho(n_curves: usize, rho_diff_tenor: ArrayView2<f64>, idx_select: &[usize]) -> Result<Array2<f64>> {
    
    let vec_rows_cols: Vec<Vec<ArrayView2<f64>>> = vec![vec![rho_diff_tenor; n_curves]; n_curves];
    let vec_rows: Result<Vec<Array2<f64>>> = vec_rows_cols
        // Now concatenate rows, in parallel
        .par_iter()
        .map(|x|
             Ok(ndarray::concatenate(Axis(1), x)
            .map_err(|_| PolarsError::ShapeMisMatch("Could not build Tenor Rho. Invalid Shape".into()))? 
            .select(Axis(1), idx_select)))
        .collect();
    
    let res = ndarray::concatenate(Axis(0), vec_rows?
        // map each "row" as view
        .iter()
        .map(|x| x.view())
        .collect::<Vec<ArrayView2<f64>>>()
        .as_slice())
    .and_then(|x| Ok(x.select(Axis(0), idx_select)))
    .map_err(|_| PolarsError::ShapeMisMatch("Could not build Tenor Rho. Invalid Shape".into()));

    res
}

/// Builds commodity: basis(location) and commodity(riskFactor) rhos
/// And name/basis for CSR
/// And potentially others
/// Removing unwanted indexes
pub(crate) fn build_basis_rho(n_tenors: usize, srs: &Series, rho_diff: f64, idx_select: &[usize]) -> Result<Array2<f64>> {
    let ln = srs.len();
    let loc_chunkarray = srs.utf8()?;
    //Keep building on each request, this is to be able to easily modify parameters
    let rho_base_same = Array2::<f64>::ones((n_tenors, n_tenors));
    let rho_base_diff = Array2::<f64>::from_elem((n_tenors, n_tenors), rho_diff);
    let mut vec_rows_cols: Vec<Vec<ArrayView2<f64>>> = vec![];

    for i in 0..ln {
        let rf_i = unsafe{ loc_chunkarray.get_unchecked(i).unwrap() };
        let rf_vec: Vec<ArrayView2<f64>> = loc_chunkarray
            .par_iter()
            .map(|x| match x {
                Some(rf2) if rf2==rf_i => rho_base_same.view() ,
                _ => rho_base_diff.view()
            })
            .collect();
        vec_rows_cols.push(rf_vec)
    }

    let vec_rows: Vec<Array2<f64>> = vec_rows_cols
        .par_iter()
        .map(|x| ndarray::concatenate(Axis(1), x)
            .unwrap()
            .select(Axis(1), idx_select))
        .collect();
    
    let rho_basis = ndarray::concatenate(Axis(0), vec_rows
            .iter()
            .map(|x| x.view())
            .collect::<Vec<ArrayView2<f64>>>()
            .as_slice())
        .and_then(|x| Ok(x.select(Axis(0), idx_select)))
        .map_err(|_| PolarsError::ShapeMisMatch("Could not build Commodity Basis Rho. Invalid Shape".into()));

    rho_basis
}
